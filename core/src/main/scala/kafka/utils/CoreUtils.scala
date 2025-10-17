/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.io.File
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import java.lang.management.ManagementFactory
import com.typesafe.scalalogging.Logger

import javax.management.ObjectName
import scala.collection.Seq
import org.apache.commons.validator.routines.InetAddressValidator
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.network.SocketServerConfigs
import org.slf4j.event.Level

import scala.jdk.CollectionConverters._

/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object CoreUtils {
  private val logger = Logger(getClass)

  private val inetAddressValidator = InetAddressValidator.getInstance()

  /**
    * Do the given action and log any exceptions thrown without rethrowing them.
    *
    * @param action The action to execute.
    * @param logging The logging instance to use for logging the thrown exception.
    * @param logLevel The log level to use for logging.
    */
  @noinline // inlining this method is not typically useful and it triggers spurious spotbugs warnings
  def swallow(action: => Unit, logging: Logging, logLevel: Level = Level.WARN): Unit = {
    try {
      action
    } catch {
      case e: Throwable => logLevel match {
        case Level.ERROR => logging.error(e.getMessage, e)
        case Level.WARN => logging.warn(e.getMessage, e)
        case Level.INFO => logging.info(e.getMessage, e)
        case Level.DEBUG => logging.debug(e.getMessage, e)
        case Level.TRACE => logging.trace(e.getMessage, e)
      }
    }
  }

  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param files list of files to be deleted
   */
  def delete(files: java.util.List[String]): Unit = files.forEach(f => Utils.delete(new File(f)))

  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer
      mbs synchronized {
        val objName = new ObjectName(name)
        if (mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register Mbean $name", e)
        false
    }
  }

  /**
   * Execute the given function inside the lock
   */
  def inLock[T](lock: Lock)(fun: => T): T = {
    lock.lock()
    try {
      fun
    } finally {
      lock.unlock()
    }
  }

  def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.readLock)(fun)

  def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)

  def listenerListToEndPoints(listeners: java.util.List[String], securityProtocolMap: java.util.Map[ListenerName, SecurityProtocol]): Seq[Endpoint] = {
    listenerListToEndPoints(listeners, securityProtocolMap, requireDistinctPorts = true)
  }

  private def checkDuplicateListenerPorts(endpoints: Seq[Endpoint], listeners: java.util.List[String]): Unit = {
    val distinctPorts = endpoints.map(_.port).distinct
    require(distinctPorts.size == endpoints.map(_.port).size, s"Each listener must have a different port, listeners: $listeners")
  }

  def listenerListToEndPoints(listeners: java.util.List[String], securityProtocolMap: java.util.Map[ListenerName, SecurityProtocol], requireDistinctPorts: Boolean): Seq[Endpoint] = {
    def validateOneIsIpv4AndOtherIpv6(first: String, second: String): Boolean =
      (inetAddressValidator.isValidInet4Address(first) && inetAddressValidator.isValidInet6Address(second)) ||
        (inetAddressValidator.isValidInet6Address(first) && inetAddressValidator.isValidInet4Address(second))

    def validate(endPoints: Seq[Endpoint]): Unit = {
      val distinctListenerNames = endPoints.map(_.listener).distinct
      require(distinctListenerNames.size == endPoints.size, s"Each listener must have a different name, listeners: $listeners")

      val (duplicatePorts, _) = endPoints.filter {
        // filter port 0 for unit tests
        ep => ep.port != 0
      }.groupBy(_.port).partition {
        case (_, endpoints) => endpoints.size > 1
      }

      // Exception case, let's allow duplicate ports if one host is on IPv4 and the other one is on IPv6
      val duplicatePortsPartitionedByValidIps = duplicatePorts.map {
        case (port, eps) =>
          (port, eps.partition(ep =>
            ep.host != null && inetAddressValidator.isValid(ep.host)
          ))
      }

      // Iterate through every grouping of duplicates by port to see if they are valid
      duplicatePortsPartitionedByValidIps.foreach {
        case (port, (duplicatesWithIpHosts, duplicatesWithoutIpHosts)) =>
          if (requireDistinctPorts)
            checkDuplicateListenerPorts(duplicatesWithoutIpHosts, listeners)

          duplicatesWithIpHosts match {
            case eps if eps.isEmpty =>
            case Seq(ep1, ep2) =>
              if (requireDistinctPorts) {
                val errorMessage = "If you have two listeners on " +
                  s"the same port then one needs to be IPv4 and the other IPv6, listeners: $listeners, port: $port"
                require(validateOneIsIpv4AndOtherIpv6(ep1.host, ep2.host), errorMessage)

                // If we reach this point it means that even though duplicatesWithIpHosts in isolation can be valid, if
                // there happens to be ANOTHER listener on this port without an IP host (such as a null host) then its
                // not valid.
                if (duplicatesWithoutIpHosts.nonEmpty)
                  throw new IllegalArgumentException(errorMessage)
              }
            case _ =>
              // Having more than 2 duplicate endpoints doesn't make sense since we only have 2 IP stacks (one is IPv4
              // and the other is IPv6)
              if (requireDistinctPorts)
                throw new IllegalArgumentException("Each listener must have a different port unless exactly one listener has " +
                  s"an IPv4 address and the other IPv6 address, listeners: $listeners, port: $port")
          }
      }
    }

    val endPoints = try {
      SocketServerConfigs.listenerListToEndPoints(listeners, securityProtocolMap).asScala
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Error creating broker listeners from '$listeners': ${e.getMessage}", e)
    }
    validate(endPoints)
    endPoints
  }
}
