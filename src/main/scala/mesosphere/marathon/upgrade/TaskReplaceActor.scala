package mesosphere.marathon.upgrade

import akka.actor._
import akka.event.EventStream
import mesosphere.marathon.TaskUpgradeCanceledException
import mesosphere.marathon.core.launchqueue.LaunchQueue
import mesosphere.marathon.core.readiness.ReadinessCheckExecutor
import mesosphere.marathon.core.task.Task
import mesosphere.marathon.core.task.Task.Id
import mesosphere.marathon.core.task.termination.{ TaskKillReason, TaskKillService }
import mesosphere.marathon.core.task.tracker.TaskTracker
import mesosphere.marathon.core.event.{ DeploymentStatus, HealthStatusChanged, MesosStatusUpdateEvent }
import mesosphere.marathon.state.AppDefinition
import mesosphere.marathon.upgrade.TaskReplaceActor._
import org.apache.mesos.Protos.TaskID
import org.apache.mesos.SchedulerDriver
import org.slf4j.LoggerFactory

import scala.collection.{ SortedSet, mutable }
import scala.concurrent.Promise

class TaskReplaceActor(
    val deploymentManager: ActorRef,
    val status: DeploymentStatus,
    val driver: SchedulerDriver,
    val killService: TaskKillService,
    val launchQueue: LaunchQueue,
    val taskTracker: TaskTracker,
    val eventBus: EventStream,
    val readinessCheckExecutor: ReadinessCheckExecutor,
    val app: AppDefinition,
    promise: Promise[Unit]) extends Actor with ReadinessBehavior with ActorLogging {

  val tasksToKill = taskTracker.appTasksLaunchedSync(app.id)
  var newTasksStarted: Int = 0
  var oldTaskIds = tasksToKill.map(_.taskId).to[SortedSet]
  val toKill = tasksToKill.to[mutable.Queue]
  var maxCapacity = (app.instances * (1 + app.upgradeStrategy.maximumOverCapacity)).toInt
  var outstandingKills = Set.empty[Task.Id]
  val CustomizationStrategy = (app.upgradeStrategy.numupgradeStrategy).ceil.toInt

  override def preStart(): Unit = {
    super.preStart()
    eventBus.subscribe(self, classOf[MesosStatusUpdateEvent])
    eventBus.subscribe(self, classOf[HealthStatusChanged])

    log.info(s"yes:enter TaskReplaceActor preStart")
    if (CustomizationStrategy > 0) {
      val ignitionStrategy = computeRestartStrategyCustomization(app, tasksToKill.size)
      log.info(s"CustomizationStrategy:ignitionStrategy.nrToKillImmediately:${ignitionStrategy.nrToKillImmediately}")
      for (_ <- 0 until ignitionStrategy.nrToKillImmediately) {
        killNextOldTask()
      }
      log.info("CustomizationStrategy:Resetting the backoff delay before restarting the app")
      launchQueue.resetDelay(app)
    } else {
      val ignitionStrategy = computeRestartStrategy(app, tasksToKill.size)
      maxCapacity = ignitionStrategy.maxCapacity
      log.info(s"yes:maxCapacity:$maxCapacity,nrToKillImmediately:${ignitionStrategy.nrToKillImmediately}")
      for (_ <- 0 until ignitionStrategy.nrToKillImmediately) {
        killNextOldTask()
      }

      reconcileNewTasks()

      log.info("Resetting the backoff delay before restarting the app")
      launchQueue.resetDelay(app)
    }
  }

  override def postStop(): Unit = {
    eventBus.unsubscribe(self)
    if (!promise.isCompleted)
      promise.tryFailure(
        new TaskUpgradeCanceledException(
          "The task upgrade has been cancelled"))
    super.postStop()
  }

  override def receive: Receive = readinessBehavior orElse replaceBehavior

  def replaceBehavior: Receive = {
    // New task failed to start, restart it
    case MesosStatusUpdateEvent(slaveId, taskId, FailedToStart(_), _, `appId`, _, _, _, `versionString`, _, _) if !oldTaskIds(taskId) => // scalastyle:ignore line.size.limit
      log.error(s"New task $taskId failed on slave $slaveId during app $appId restart")
      taskTerminated(taskId)
      launchQueue.add(app)

    // Old task successfully killed
    case MesosStatusUpdateEvent(slaveId, taskId, KillComplete(_), _, `appId`, _, _, _, _, _, _) if oldTaskIds(taskId) => // scalastyle:ignore line.size.limit

      log.info(s"yes: New task $taskId updateEvent on slave $slaveId, $appId, $taskId")
      oldTaskIds -= taskId
      outstandingKills -= taskId
      reconcileNewTasks()
      checkFinished()

    case x: Any => log.debug(s"Received $x")
  }

  override def taskStatusChanged(taskId: Id): Unit = {
    log.info(s"yes:enter taskStatusChanged:$taskId")
    if (healthyTasks(taskId) && readyTasks(taskId)) killNextOldTask(Some(taskId))
    checkFinished()
  }

  def reconcileNewTasks(): Unit = {
    if (CustomizationStrategy > 0) {
      val tasksNotStartedYet = math.max(0, app.instances - newTasksStarted)
      var tasksToStartNow = 0
      if (oldTaskIds.size == 0) {
        log.info(s"yes:oldTaskIds.size == 0:tasksNotStartedYet:$tasksNotStartedYet")
        //tasksToStartNow = math.min(tasksNotStartedYet, CustomizationStrategy)
        tasksToStartNow = tasksNotStartedYet;
      } else {
        log.info(s"yes:oldTaskIds.size:${oldTaskIds.size},tasksNotStartedYet:$tasksNotStartedYet")
        tasksToStartNow = math.min(tasksNotStartedYet, 1)
      }

      if (tasksToStartNow > 0) {
        log.info(s"yes:reconciling tasks during app $appId restart: queuing $tasksToStartNow new tasks")
        launchQueue.add(app, tasksToStartNow)
        newTasksStarted += tasksToStartNow
      }

    } else {
      val leftCapacity = math.max(0, maxCapacity - oldTaskIds.size - newTasksStarted)
      val tasksNotStartedYet = math.max(0, app.instances - newTasksStarted)
      val tasksToStartNow = math.min(tasksNotStartedYet, leftCapacity)
      log.info(s"yes:reconcileNewTasks:maxCapacity:$maxCapacity,oldTaskIds.size:$oldTaskIds.size," +
        s"newTasksStarted:$newTasksStarted,app.instances:${app.instances},so:leftCapacity:$leftCapacity," +
        s"tasksNotStartedYet:$tasksNotStartedYet,tasksToStartNow:$tasksToStartNow")

      if (tasksToStartNow > 0) {
        log.info(s"Reconciling tasks during app $appId restart: queuing $tasksToStartNow new tasks")
        launchQueue.add(app, tasksToStartNow)
        newTasksStarted += tasksToStartNow
      }
    }
  }

  def killNextOldTask(maybeNewTaskId: Option[Task.Id] = None): Unit = {
    log.info(s"yes:enter killNextOldTask")
    if (toKill.nonEmpty) {
      val nextOldTask = toKill.dequeue()
      log.info(s"ready killTask:${nextOldTask.taskId}")
      maybeNewTaskId match {
        case Some(newTaskId: Task.Id) =>
          log.info(s"Killing old $nextOldTask because $newTaskId became reachable")
        case _ =>
          log.info(s"Killing old $nextOldTask")
      }

      outstandingKills += nextOldTask.taskId
      killService.killTask(nextOldTask, TaskKillReason.Upgrading)
    } else if (CustomizationStrategy > 0) {
      //reconcileNewTasks()
    }
  }

  def checkFinished(): Unit = {
    if (taskTargetCountReached(app.instances) && oldTaskIds.isEmpty) {
      log.info(s"App All new tasks for $appId are ready and all old tasks have been killed")
      promise.success(())
      context.stop(self)
    } else if (log.isDebugEnabled) {
      log.debug(s"For app: [${app.id}] there are [${healthyTasks.size}] healthy and " +
        s"[${readyTasks.size}] ready new instances and " +
        s"[${oldTaskIds.size}] old instances.")

      log.info(s"For app: [${app.id}] there are [${healthyTasks.size}] healthy and " +
        s"[${readyTasks.size}] ready new instances and " + s"[${oldTaskIds.size}] old instances.")
    } else {
      log.info(s"For app: [${app.id}] there are [${healthyTasks.size}] healthy and " +
        s"[${readyTasks.size}] ready new instances and " + s"[${oldTaskIds.size}] old instances.")
    }
  }

  def buildTaskId(id: String): TaskID =
    TaskID.newBuilder()
      .setValue(id)
      .build()
}

object TaskReplaceActor {
  private[this] val log = LoggerFactory.getLogger(getClass)

  val KillComplete = "^TASK_(ERROR|FAILED|FINISHED|LOST|KILLED)$".r
  val FailedToStart = "^TASK_(ERROR|FAILED|LOST|KILLED)$".r

  //scalastyle:off
  def props(
    deploymentManager: ActorRef,
    status: DeploymentStatus,
    driver: SchedulerDriver,
    killService: TaskKillService,
    launchQueue: LaunchQueue,
    taskTracker: TaskTracker,
    eventBus: EventStream,
    readinessCheckExecutor: ReadinessCheckExecutor,
    app: AppDefinition,
    promise: Promise[Unit]): Props = Props(
    new TaskReplaceActor(deploymentManager, status, driver, killService, launchQueue, taskTracker, eventBus,
      readinessCheckExecutor, app, promise)
  )

  /** Encapsulates the logic how to get a Restart going */
  private[upgrade] case class RestartStrategy(nrToKillImmediately: Int, maxCapacity: Int)

  private[upgrade] def computeRestartStrategy(app: AppDefinition, runningTasksCount: Int): RestartStrategy = {
    // in addition to an app definition which passed validation, we require:
    require(app.instances > 0, s"instances must be > 0 but is ${app.instances}")
    require(runningTasksCount >= 0, s"running task count must be >=0 but is $runningTasksCount")
    //val numUpgradeOneTime = (app.upgradeStrategy.numupgradeStrategy).ceil.toInt
    val minHealthy = (app.instances * app.upgradeStrategy.minimumHealthCapacity).ceil.toInt
    var maxCapacity = (app.instances * (1 + app.upgradeStrategy.maximumOverCapacity)).toInt
    var nrToKillImmediately = math.max(0, runningTasksCount - minHealthy)
    //log.info(s"yes:fuck numupgradeStrategy:${app.upgradeStrategy.numupgradeStrategy},numUpgradeOneTime:$numUpgradeOneTime")
    log.info(s"yes:enter computeRestartStrategy:app.instances:${app.instances},runningTasksCount:$runningTasksCount,app.upgradeStrategy.minimumHealthCapacity:${app.upgradeStrategy.minimumHealthCapacity},app.upgradeStrategy.maximumOverCapacity:${app.upgradeStrategy.maximumOverCapacity},so minHealthy:$minHealthy,maxCapacity:$maxCapacity,nrToKillImmediately:$nrToKillImmediately")

    if (app.isResident) {
      log.info(s"yes:app.isResident is true")
    } else {
      log.info(s"yes:app.isResident is false")
    }
    if (minHealthy == maxCapacity && maxCapacity <= runningTasksCount) {
      if (app.isResident) {
        // Kill enough tasks so that we end up with end up with one task below minHealthy.
        // TODO: We need to do this also while restarting, since the kill could get lost.
        nrToKillImmediately = runningTasksCount - minHealthy + 1
        log.info(
          "maxCapacity == minHealthy for resident app: " +
            s"adjusting nrToKillImmediately to $nrToKillImmediately in order to prevent over-capacity for resident app"
        )
      } else {
        log.info(s"maxCapacity == minHealthy: Allow temporary over-capacity of one task to allow restarting")
        maxCapacity += 1
      }
    }

    log.info(s"For minimumHealthCapacity ${app.upgradeStrategy.minimumHealthCapacity} of ${app.id.toString} leave " +
      s"$minHealthy tasks running, maximum capacity $maxCapacity, killing $nrToKillImmediately of " +
      s"$runningTasksCount running tasks immediately")

    assume(nrToKillImmediately >= 0, s"nrToKillImmediately must be >=0 but is $nrToKillImmediately")
    assume(maxCapacity > 0, s"maxCapacity must be >0 but is $maxCapacity")
    def canStartNewTasks: Boolean = minHealthy < maxCapacity || runningTasksCount - nrToKillImmediately < maxCapacity
    assume(canStartNewTasks, s"must be able to start new tasks")

    RestartStrategy(nrToKillImmediately = nrToKillImmediately, maxCapacity = maxCapacity)
  }

  private[upgrade] def computeRestartStrategyCustomization(app: AppDefinition, runningTasksCount: Int): RestartStrategy = {
    log.info(s"yes:enter computeRestartStrategyCustomization");
    val numUpgradeOneTime = (app.upgradeStrategy.numupgradeStrategy).ceil.toInt
    var nrToKillImmediately = numUpgradeOneTime
    if (runningTasksCount > app.instances) {
      nrToKillImmediately = math.max(runningTasksCount - app.instances, numUpgradeOneTime)
    }
    var maxCapacity = math.max(app.instances, runningTasksCount)
    log.info(s"yes:fuck nrToKillImmediately:$nrToKillImmediately,app.instances:${app.instances},runningTasksCount:$runningTasksCount")
    RestartStrategy(nrToKillImmediately, maxCapacity)
  }

}

