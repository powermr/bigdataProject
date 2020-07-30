import org.apache.log4j.Level
import org.apache.log4j.Logger

trait Loggable {

  @transient private lazy val logger = Logger.getLogger(getClass)

  @transient protected lazy val sparkdemoLogger: Logger = Logger.getLogger("sparkDemo")

  def getGriffinLogLevel: Level = {
    var logger = sparkdemoLogger
    while (logger != null && logger.getLevel == null) {
      logger = logger.getParent.asInstanceOf[Logger]
    }
    logger.getLevel
  }

  protected def info(msg: => String): Unit = {
    logger.info(msg)
  }

  protected def debug(msg: => String): Unit = {
    logger.debug(msg)
  }

  protected def warn(msg: => String): Unit = {
    logger.warn(msg)
  }

  protected def warn(msg: => String, e: Throwable): Unit = {
    logger.warn(msg, e)
  }

  protected def error(msg: => String): Unit = {
    logger.error(msg)
  }

  protected def error(msg: => String, e: Throwable): Unit = {
    logger.error(msg, e)
  }

}