/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pmem;

import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlasmaStoreServer {

  private static final Logger logger = LoggerFactory.getLogger(PlasmaStoreServer.class);

  public final static String plasmaStoreServer = "plasma-store-server";

  public final static String plasmaStoreSocket = SparkEnv.get() == null ?
      PlasmaConf.DEFAULT_STORE_SERVER_SOCKET_VALUE :
      SparkEnv.get().conf().get(PlasmaConf.STORE_SERVER_SOCKET_KEY);

  public final static String plasmaStoreDir = SparkEnv.get() == null ?
      PlasmaConf.DEFAULT_STORE_SERVER_DIR_VALUE :
      SparkEnv.get().conf().get(PlasmaConf.STORE_SERVER_DIR_KEY);

  public final static String plasmaStoreMemory = SparkEnv.get() == null ?
      PlasmaConf.DEFAULT_STORE_SERVER_MEMORY_VALUE :
      SparkEnv.get().conf().get(PlasmaConf.STORE_SERVER_MEMORY_KEY);

  public static Process process;

  public static boolean isPlasmaJavaAvailable() {
    boolean available = true;
    try {
      System.loadLibrary("plasma_java");
    } catch (UnsatisfiedLinkError ex) {
      available = false;
    }
    return available;
  }

  public static boolean isPlasmaStoreExist() {
    return Stream.of(System.getenv("PATH").split(Pattern.quote(File.pathSeparator)))
        .map(Paths::get)
        .anyMatch(path -> Files.exists(path.resolve(plasmaStoreServer)));
  }

  public static Process startProcess(String command) throws IOException {
    List<String> cmdList = Arrays.stream(command.split(" ")).collect(Collectors.toList());
    ProcessBuilder processBuilder = new ProcessBuilder(cmdList).inheritIO();
    Process process = processBuilder.start();
    return process;
  }

  public static void startPlasmaStoreWithLock() throws IOException, InterruptedException {
    if (!isPlasmaJavaAvailable() || !isPlasmaStoreExist()) {
      logger.info("Please make sure plasma store server is installed.");
      return;
    }

    if (PlasmaStoreLockFile.lock()) {
      logger.info("Starting plasma store server...");
      startPlasmaStore();
    } else {
      logger.info("Plasma store server is already started.");
    }
  }

  public static boolean startPlasmaStore() throws IOException, InterruptedException {
    String command = plasmaStoreServer + " -s " + plasmaStoreSocket
        + " -m " + plasmaStoreMemory + " -d " + plasmaStoreDir;
    process = startProcess(command);
    int ticktock = 60;
    if (process != null) {
      while (!process.isAlive()) {
        TimeUnit.MILLISECONDS.sleep(1000);
        ticktock--;
        if (ticktock == 0 && !process.isAlive()) {
          throw new RuntimeException("Failed to start plasma store server");
        }
      }
      return true;
    }
    return false;
  }

  public static void stopPlasmaStore() throws InterruptedException {
    if (process != null && process.isAlive()) {
      process.destroyForcibly();
      int ticktock = 60;
      while (process.isAlive()) {
        TimeUnit.MILLISECONDS.sleep(1000);
        ticktock--;
        if (ticktock == 0 && process.isAlive()) {
          throw new RuntimeException("Failed to stop plasma store server");
        }
      }
    }
  }

  public static void deletePlasmaSocketFile() {
    File socketFile = new File(plasmaStoreSocket);
    if (socketFile != null && socketFile.exists()) {
      socketFile.delete();
    }
  }

}
