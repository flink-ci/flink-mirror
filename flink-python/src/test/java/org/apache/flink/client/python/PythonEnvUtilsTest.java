/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.python;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.OperatingSystem;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.client.python.PythonEnvUtils.PYFLINK_CLIENT_EXECUTABLE;
import static org.apache.flink.client.python.PythonEnvUtils.preparePythonEnvironment;
import static org.apache.flink.python.PythonOptions.PYTHON_ARCHIVES;
import static org.apache.flink.python.PythonOptions.PYTHON_CLIENT_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_FILES;
import static org.apache.flink.python.util.PythonDependencyUtils.FILE_DELIMITER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/** Tests for the {@link PythonEnvUtils}. */
class PythonEnvUtilsTest {

    private String tmpDirPath;

    @BeforeEach
    void prepareTestEnvironment() {
        File tmpDirFile =
                new File(System.getProperty("java.io.tmpdir"), "pyflink_" + UUID.randomUUID());
        tmpDirFile.mkdirs();
        this.tmpDirPath = tmpDirFile.getAbsolutePath();
    }

    @Test
    void testPreparePythonEnvironment() throws IOException {
        // Skip this test on Windows as we can not control the Window Driver letters.
        assumeThat(OperatingSystem.isWindows()).isFalse();

        // xxx/a.zip, xxx/subdir/b.py, xxx/subdir/c.zip
        File zipFile = new File(tmpDirPath + File.separator + "a.zip");
        File dirFile = new File(tmpDirPath + File.separator + "module_dir");
        File subdirFile = new File(tmpDirPath + File.separator + "subdir");
        File relativeFile =
                new File(tmpDirPath + File.separator + "subdir" + File.separator + "b.py");
        File schemeFile =
                new File(tmpDirPath + File.separator + "subdir" + File.separator + "c.egg");

        // The files must actually exist
        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            ZipArchiveEntry entry = new ZipArchiveEntry("zipDir" + "/zipfile0");
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {1, 1, 1, 1, 1});
            zipOut.closeArchiveEntry();
        }
        dirFile.mkdir();
        subdirFile.mkdir();
        relativeFile.createNewFile();
        schemeFile.createNewFile();

        String workingDir = new File("").getAbsolutePath();
        String absolutePath = relativeFile.getAbsolutePath();

        Path zipPath = new Path(zipFile.getAbsolutePath());
        Path dirPath = new Path(dirFile.getAbsolutePath());
        Path relativePath =
                new Path(Paths.get(workingDir).relativize(Paths.get(absolutePath)).toString());
        Path schemePath = new Path("file://" + schemeFile.getAbsolutePath());

        List<Path> pyFilesList = new ArrayList<>();
        pyFilesList.add(zipPath);
        pyFilesList.add(dirPath);
        pyFilesList.add(relativePath);
        pyFilesList.add(schemePath);

        String pyFiles =
                pyFilesList.stream()
                        .map(Path::toString)
                        .collect(Collectors.joining(FILE_DELIMITER));

        Configuration config = new Configuration();
        config.set(PYTHON_FILES, pyFiles);

        PythonEnvUtils.PythonEnvironment env = preparePythonEnvironment(config, null, tmpDirPath);

        String base = replaceUUID(env.tempDirectory);
        Set<String> expectedPythonPaths = new HashSet<>();
        expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "a"));
        expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "module_dir"));
        expectedPythonPaths.add(String.join(File.separator, base, "{uuid}"));
        expectedPythonPaths.add(String.join(File.separator, base, "{uuid}", "c.egg"));

        Set<String> actualPaths =
                Arrays.stream(env.pythonPath.split(File.pathSeparator))
                        .map(PythonEnvUtilsTest::replaceUUID)
                        .collect(Collectors.toSet());
        assertThat(actualPaths).isEqualTo(expectedPythonPaths);
    }

    @Test
    void testStartPythonProcess() {
        PythonEnvUtils.PythonEnvironment pythonEnv = new PythonEnvUtils.PythonEnvironment();
        pythonEnv.tempDirectory = tmpDirPath;
        pythonEnv.pythonPath = tmpDirPath;
        List<String> commands = new ArrayList<>();
        String pyPath = String.join(File.separator, tmpDirPath, "verifier.py");
        try {
            File pyFile = new File(pyPath);
            pyFile.createNewFile();
            pyFile.setExecutable(true);
            String pyProgram =
                    "#!/usr/bin/python\n"
                            + "# -*- coding: UTF-8 -*-\n"
                            + "import os\n"
                            + "import sys\n"
                            + "\n"
                            + "if __name__=='__main__':\n"
                            + "\tfilename = sys.argv[1]\n"
                            + "\tfo = open(filename, \"w\")\n"
                            + "\tfo.write(os.getcwd())\n"
                            + "\tfo.close()";
            Files.write(pyFile.toPath(), pyProgram.getBytes(), StandardOpenOption.WRITE);
            String result = String.join(File.separator, tmpDirPath, "python_working_directory.txt");
            commands.add(pyPath);
            commands.add(result);
            Process pythonProcess = PythonEnvUtils.startPythonProcess(pythonEnv, commands, false);
            int exitCode = pythonProcess.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Python process exits with code: " + exitCode);
            }
            String cmdResult = new String(Files.readAllBytes(new File(result).toPath()));
            // Check if the working directory of python process is the same as java process.
            assertThat(cmdResult).isEqualTo(System.getProperty("user.dir"));
            pythonProcess.destroyForcibly();
            pyFile.delete();
            new File(result).delete();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("test start Python process failed " + e.getMessage());
        }
    }

    @Test
    void testSetPythonExecutable() throws IOException {
        Configuration config = new Configuration();

        File zipFile = new File(tmpDirPath + File.separator + "venv.zip");
        try (ZipArchiveOutputStream zipOut =
                new ZipArchiveOutputStream(new FileOutputStream(zipFile))) {
            ZipArchiveEntry entry = new ZipArchiveEntry("zipDir" + "/zipfile0");
            zipOut.putArchiveEntry(entry);
            zipOut.write(new byte[] {1, 1, 1, 1, 1});
            zipOut.closeArchiveEntry();
        }
        PythonEnvUtils.PythonEnvironment env = preparePythonEnvironment(config, null, tmpDirPath);
        if (OperatingSystem.isWindows()) {
            assertThat(env.pythonExec).isEqualTo("python.exe");
        } else {
            assertThat(env.pythonExec).isEqualTo("python");
        }

        Map<String, String> systemEnv = new HashMap<>(System.getenv());
        systemEnv.put(PYFLINK_CLIENT_EXECUTABLE, "python3");
        CommonTestUtils.setEnv(systemEnv);
        try {
            env = preparePythonEnvironment(config, null, tmpDirPath);
            assertThat(env.pythonExec).isEqualTo("python3");
        } finally {
            systemEnv.remove(PYFLINK_CLIENT_EXECUTABLE);
            CommonTestUtils.setEnv(systemEnv);
        }

        config.set(PYTHON_ARCHIVES, zipFile.getPath());
        systemEnv = new HashMap<>(System.getenv());
        systemEnv.put(PYFLINK_CLIENT_EXECUTABLE, "venv.zip/venv/bin/python");
        CommonTestUtils.setEnv(systemEnv);
        try {
            env = preparePythonEnvironment(config, null, tmpDirPath);
            assertThat(env.pythonExec).isEqualTo("venv.zip/venv/bin/python");
        } finally {
            systemEnv.remove(PYFLINK_CLIENT_EXECUTABLE);
            CommonTestUtils.setEnv(systemEnv);
        }
        java.nio.file.Path[] files =
                FileUtils.listDirectory(new File(env.archivesDirectory).toPath());
        assertThat(files).hasSize(1);
        assertThat(files[0].getFileName().toString()).isEqualTo(zipFile.getName());

        config.removeConfig(PYTHON_ARCHIVES);

        config.set(PYTHON_CLIENT_EXECUTABLE, "/usr/bin/python");
        env = preparePythonEnvironment(config, null, tmpDirPath);
        assertThat(env.pythonExec).isEqualTo("/usr/bin/python");
    }

    @Test
    void testPrepareEnvironmentWithEntryPointScript() throws IOException {
        File entryFile = new File(tmpDirPath + File.separator + "test.py");
        // The file must actually exist
        entryFile.createNewFile();
        String entryFilePath = entryFile.getAbsolutePath();

        Configuration config = new Configuration();
        PythonEnvUtils.PythonEnvironment env =
                preparePythonEnvironment(config, entryFilePath, tmpDirPath);

        Set<String> expectedPythonPaths = new HashSet<>();
        expectedPythonPaths.add(
                new Path(String.join(File.separator, replaceUUID(env.tempDirectory), "{uuid}"))
                        .toString());

        Set<String> actualPaths =
                Arrays.stream(env.pythonPath.split(File.pathSeparator))
                        .map(PythonEnvUtilsTest::replaceUUID)
                        .collect(Collectors.toSet());
        assertThat(actualPaths).isEqualTo(expectedPythonPaths);
    }

    @AfterEach
    void cleanEnvironment() {
        FileUtils.deleteDirectoryQuietly(new File(tmpDirPath));
    }

    private static String replaceUUID(String originPath) {
        return originPath.replaceAll(
                "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}", "{uuid}");
    }
}
