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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.fs.local.LocalRecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpCommitter;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverableFsDataOutputStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.utils.NoOpRecoverableWriter;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.HamcrestCondition.matching;

/** Tests for the {@code Bucket}. */
class BucketTest {

    @TempDir private static java.nio.file.Path tempFolder;

    @Test
    void shouldNotCleanupResumablesThatArePartOfTheAckedCheckpoint() throws IOException {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
        final Bucket<String, String> bucketUnderTest =
                createBucket(recoverableWriter, path, 0, 0, OutputFileConfig.builder().build());

        bucketUnderTest.write("test-element", 0L);

        final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
        assertThat(state).is(matching(hasActiveInProgressFile()));

        bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);
        assertThat(recoverableWriter)
                .is(matching(hasCalledDiscard(0))); // it did not discard as this is still valid.
    }

    @Test
    void shouldCleanupOutdatedResumablesOnCheckpointAck() throws IOException {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
        final Bucket<String, String> bucketUnderTest =
                createBucket(recoverableWriter, path, 0, 0, OutputFileConfig.builder().build());

        bucketUnderTest.write("test-element", 0L);

        final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
        assertThat(state).is(matching(hasActiveInProgressFile()));

        bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);

        bucketUnderTest.onReceptionOfCheckpoint(1L);
        bucketUnderTest.onReceptionOfCheckpoint(2L);

        bucketUnderTest.onSuccessfulCompletionOfCheckpoint(2L);
        assertThat(recoverableWriter)
                .is(matching(hasCalledDiscard(2))); // that is for checkpoints 0 and 1
    }

    @Test
    void shouldNotCallCleanupWithoutInProgressPartFiles() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
        final Bucket<String, String> bucketUnderTest =
                createBucket(recoverableWriter, path, 0, 0, OutputFileConfig.builder().build());

        final BucketState<String> state = bucketUnderTest.onReceptionOfCheckpoint(0L);
        assertThat(state).is(matching(hasNoActiveInProgressFile()));

        bucketUnderTest.onReceptionOfCheckpoint(1L);
        bucketUnderTest.onReceptionOfCheckpoint(2L);

        bucketUnderTest.onSuccessfulCompletionOfCheckpoint(2L);
        assertThat(recoverableWriter)
                .is(matching(hasCalledDiscard(0))); // we have no in-progress file.
    }

    @Test
    void shouldCleanupOutdatedResumablesAfterResumed() throws Exception {
        final File outDir = TempDirUtils.newFolder(tempFolder);
        final Path path = new Path(outDir.toURI());

        final TestRecoverableWriter recoverableWriter = getRecoverableWriter(path);
        final Bucket<String, String> bucketUnderTest =
                createBucket(recoverableWriter, path, 0, 0, OutputFileConfig.builder().build());

        bucketUnderTest.write("test-element", 0L);
        final BucketState<String> state0 = bucketUnderTest.onReceptionOfCheckpoint(0L);
        assertThat(state0).is(matching(hasActiveInProgressFile()));
        bucketUnderTest.onSuccessfulCompletionOfCheckpoint(0L);
        assertThat(recoverableWriter).is(matching(hasCalledDiscard(0)));

        final File newOutDir = TempDirUtils.newFolder(tempFolder);
        final Path newPath = new Path(newOutDir.toURI());
        final TestRecoverableWriter newRecoverableWriter = getRecoverableWriter(newPath);
        final Bucket<String, String> bucketAfterResume =
                restoreBucket(
                        newRecoverableWriter, 0, 0, state0, OutputFileConfig.builder().build());
        final BucketState<String> state1 = bucketAfterResume.onReceptionOfCheckpoint(1L);
        assertThat(state1).is(matching(hasActiveInProgressFile()));
        bucketAfterResume.onSuccessfulCompletionOfCheckpoint(1L);
        assertThat(newRecoverableWriter).is(matching(hasCalledDiscard(1)));
    }

    // --------------------------- Checking Restore ---------------------------

    @Test
    void inProgressFileShouldBeCommittedIfWriterDoesNotSupportResume() throws IOException {
        final StubNonResumableWriter nonResumableWriter = new StubNonResumableWriter();
        final Bucket<String, String> bucket =
                getRestoredBucketWithOnlyInProgressPart(nonResumableWriter);

        assertThat(nonResumableWriter).is(matching(hasMethodCallCountersEqualTo(1, 0, 1)));
        assertThat(bucket).is(matching(hasNullInProgressFile(true)));
    }

    @Test
    void inProgressFileShouldBeRestoredIfWriterSupportsResume() throws IOException {
        final StubResumableWriter resumableWriter = new StubResumableWriter();
        final Bucket<String, String> bucket =
                getRestoredBucketWithOnlyInProgressPart(resumableWriter);

        assertThat(resumableWriter).is(matching(hasMethodCallCountersEqualTo(1, 1, 0)));
        assertThat(bucket).is(matching(hasNullInProgressFile(false)));
    }

    @Test
    void pendingFilesShouldBeRestored() throws IOException {
        final int expectedRecoverForCommitCounter = 10;

        final StubNonResumableWriter writer = new StubNonResumableWriter();
        final Bucket<String, String> bucket =
                getRestoredBucketWithOnlyPendingParts(writer, expectedRecoverForCommitCounter);

        assertThat(writer)
                .is(matching(hasMethodCallCountersEqualTo(0, 0, expectedRecoverForCommitCounter)));
        assertThat(bucket).is(matching(hasNullInProgressFile(true)));
    }

    // ------------------------------- Matchers --------------------------------

    private static TypeSafeMatcher<TestRecoverableWriter> hasCalledDiscard(int times) {
        return new TypeSafeMatcher<TestRecoverableWriter>() {
            @Override
            protected boolean matchesSafely(TestRecoverableWriter writer) {
                return writer.getCleanupCallCounter() == times;
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText(
                                "the TestRecoverableWriter to have called discardRecoverableState() ")
                        .appendValue(times)
                        .appendText(" times.");
            }
        };
    }

    private static TypeSafeMatcher<BucketState<String>> hasActiveInProgressFile() {
        return new TypeSafeMatcher<BucketState<String>>() {
            @Override
            protected boolean matchesSafely(BucketState<String> state) {
                return state.getInProgressFileRecoverable() != null;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a BucketState with active in-progress file.");
            }
        };
    }

    private static TypeSafeMatcher<BucketState<String>> hasNoActiveInProgressFile() {
        return new TypeSafeMatcher<BucketState<String>>() {
            @Override
            protected boolean matchesSafely(BucketState<String> state) {
                return state.getInProgressFileRecoverable() == null;
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("a BucketState with no active in-progress file.");
            }
        };
    }

    private static TypeSafeMatcher<Bucket<String, String>> hasNullInProgressFile(
            final boolean isNull) {

        return new TypeSafeMatcher<Bucket<String, String>>() {
            @Override
            protected boolean matchesSafely(Bucket<String, String> bucket) {
                final InProgressFileWriter<String, String> inProgressPart =
                        bucket.getInProgressPart();
                return isNull == (inProgressPart == null);
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("a Bucket with its inProgressPart being ")
                        .appendText(isNull ? " null." : " not null.");
            }
        };
    }

    private static TypeSafeMatcher<BaseStubWriter> hasMethodCallCountersEqualTo(
            final int supportsResumeCalls,
            final int recoverCalls,
            final int recoverForCommitCalls) {

        return new TypeSafeMatcher<BaseStubWriter>() {
            @Override
            protected boolean matchesSafely(BaseStubWriter writer) {
                return writer.getSupportsResumeCallCounter() == supportsResumeCalls
                        && writer.getRecoverCallCounter() == recoverCalls
                        && writer.getRecoverForCommitCallCounter() == recoverForCommitCalls;
            }

            @Override
            public void describeTo(Description description) {
                description
                        .appendText("a Writer where:")
                        .appendText(" supportsResume was called ")
                        .appendValue(supportsResumeCalls)
                        .appendText(" times,")
                        .appendText(" recover was called ")
                        .appendValue(recoverCalls)
                        .appendText(" times,")
                        .appendText(" and recoverForCommit was called ")
                        .appendValue(recoverForCommitCalls)
                        .appendText(" times.")
                        .appendText("'");
            }
        };
    }

    // ------------------------------- Mock Classes --------------------------------

    private static class TestRecoverableWriter extends LocalRecoverableWriter {

        private int cleanupCallCounter = 0;

        TestRecoverableWriter(LocalFileSystem fs) {
            super(fs);
        }

        int getCleanupCallCounter() {
            return cleanupCallCounter;
        }

        @Override
        public boolean requiresCleanupOfRecoverableState() {
            // here we return true so that the cleanupRecoverableState() is called.
            return true;
        }

        @Override
        public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
            cleanupCallCounter++;
            return false;
        }

        @Override
        public String toString() {
            return "TestRecoverableWriter has called discardRecoverableState() "
                    + cleanupCallCounter
                    + " times.";
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class StubResumableWriter extends BaseStubWriter {

        StubResumableWriter() {
            super(true);
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class StubNonResumableWriter extends BaseStubWriter {

        StubNonResumableWriter() {
            super(false);
        }
    }

    /**
     * A test implementation of a {@link RecoverableWriter} that does not support resuming, i.e.
     * keep on writing to the in-progress file at the point we were before the failure.
     */
    private static class BaseStubWriter extends NoOpRecoverableWriter {

        private final boolean supportsResume;

        private int supportsResumeCallCounter = 0;

        private int recoverCallCounter = 0;

        private int recoverForCommitCallCounter = 0;

        private BaseStubWriter(final boolean supportsResume) {
            this.supportsResume = supportsResume;
        }

        int getSupportsResumeCallCounter() {
            return supportsResumeCallCounter;
        }

        int getRecoverCallCounter() {
            return recoverCallCounter;
        }

        int getRecoverForCommitCallCounter() {
            return recoverForCommitCallCounter;
        }

        @Override
        public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable)
                throws IOException {
            recoverCallCounter++;
            return new NoOpRecoverableFsDataOutputStream();
        }

        @Override
        public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
                throws IOException {
            checkArgument(resumable instanceof NoOpRecoverable);
            recoverForCommitCallCounter++;
            return new NoOpCommitter();
        }

        @Override
        public boolean supportsResume() {
            supportsResumeCallCounter++;
            return supportsResume;
        }
    }

    // ------------------------------- Utility Methods --------------------------------

    private static final String bucketId = "testing-bucket";

    private static final RollingPolicy<String, String> rollingPolicy =
            DefaultRollingPolicy.builder().build();

    private static final Encoder ENCODER = new SimpleStringEncoder<>();

    private static Bucket<String, String> createBucket(
            final RecoverableWriter writer,
            final Path bucketPath,
            final int subtaskIdx,
            final int initialPartCounter,
            final OutputFileConfig outputFileConfig)
            throws IOException {

        return Bucket.getNew(
                subtaskIdx,
                bucketId,
                bucketPath,
                initialPartCounter,
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                null,
                outputFileConfig);
    }

    private static Bucket<String, String> restoreBucket(
            final RecoverableWriter writer,
            final int subtaskIndex,
            final long initialPartCounter,
            final BucketState<String> bucketState,
            final OutputFileConfig outputFileConfig)
            throws Exception {

        return Bucket.restore(
                subtaskIndex,
                initialPartCounter,
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                bucketState,
                null,
                outputFileConfig);
    }

    private static TestRecoverableWriter getRecoverableWriter(Path path) throws IOException {
        final FileSystem fs = FileSystem.get(path.toUri());
        assertThat(fs)
                .as("Expected Local FS but got a " + fs.getClass().getName() + " for path: " + path)
                .isInstanceOf(LocalFileSystem.class);
        return new TestRecoverableWriter((LocalFileSystem) fs);
    }

    private Bucket<String, String> getRestoredBucketWithOnlyInProgressPart(
            final BaseStubWriter writer) throws IOException {
        final BucketState<String> stateWithOnlyInProgressFile =
                new BucketState<>(
                        "test",
                        new Path(),
                        12345L,
                        new OutputStreamBasedPartFileWriter
                                .OutputStreamBasedInProgressFileRecoverable(new NoOpRecoverable()),
                        new HashMap<>());

        return Bucket.restore(
                0,
                1L,
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                stateWithOnlyInProgressFile,
                null,
                OutputFileConfig.builder().build());
    }

    private Bucket<String, String> getRestoredBucketWithOnlyPendingParts(
            final BaseStubWriter writer, final int numberOfPendingParts) throws IOException {
        final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                completePartsPerCheckpoint = createPendingPartsPerCheckpoint(numberOfPendingParts);

        final BucketState<String> initStateWithOnlyInProgressFile =
                new BucketState<>("test", new Path(), 12345L, null, completePartsPerCheckpoint);
        return Bucket.restore(
                0,
                1L,
                new RowWiseBucketWriter<>(writer, ENCODER),
                rollingPolicy,
                initStateWithOnlyInProgressFile,
                null,
                OutputFileConfig.builder().build());
    }

    private Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
            createPendingPartsPerCheckpoint(int noOfCheckpoints) {
        final Map<Long, List<InProgressFileWriter.PendingFileRecoverable>>
                pendingCommittablesPerCheckpoint = new HashMap<>();
        for (int checkpointId = 0; checkpointId < noOfCheckpoints; checkpointId++) {
            final List<InProgressFileWriter.PendingFileRecoverable> pending = new ArrayList<>();
            pending.add(
                    new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable(
                            new NoOpRecoverable()));
            pendingCommittablesPerCheckpoint.put((long) checkpointId, pending);
        }
        return pendingCommittablesPerCheckpoint;
    }
}
