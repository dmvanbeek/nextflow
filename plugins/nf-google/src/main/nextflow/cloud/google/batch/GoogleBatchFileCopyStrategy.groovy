/*
 * Copyright 2013-2026, Seqera Labs
 *
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

package nextflow.cloud.google.batch

import java.nio.file.Path

import com.google.cloud.batch.v1.Volume
import com.google.cloud.storage.contrib.nio.CloudStoragePath
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.cloud.google.GoogleOpts
import nextflow.cloud.google.util.GcsBashLib
import nextflow.executor.BashWrapperBuilder
import nextflow.processor.TaskBean
import nextflow.processor.TaskRun
import nextflow.util.Duration
import nextflow.util.Escape

/**
 * Implements a file copy strategy for Google Batch using 'gcloud storage cp'.
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
@CompileStatic
class GoogleBatchFileCopyStrategy extends BashWrapperBuilder implements GoogleBatchLauncherSpec {

    private final GoogleOpts opts
    private int maxParallelTransfers = 16
    private int maxTransferAttempts = 5
    private Duration delayBetweenAttempts = Duration.of('10s')

    GoogleBatchFileCopyStrategy(TaskBean task, GoogleOpts opts) {
        super(task)
        this.opts = opts
    }

    @Override
    String getBeforeStartScript() {
        return GcsBashLib.script(maxParallelTransfers, maxTransferAttempts, delayBetweenAttempts)
    }

    @Override
    String getStageInputFilesScript(Map<String,Path> inputFiles) {
        def result = 'downloads=(true)\n'
        result += super.getStageInputFilesScript(inputFiles) + '\n'
        result += 'nxf_parallel "${downloads[@]}"\n'
        return result
    }

    @Override
    String stageInputFile( Path path, String targetName ) {
        def remotePath = toUri(path)
        return "downloads+=(\"nxf_gcs_download '$remotePath' '$targetName'\")"
    }

    @Override
    String getUnstageOutputFilesScript(List<String> outputFiles, Path targetDir) {
        final patterns = normalizeGlobStarPaths(outputFiles)
        if (!patterns) {
            return null
        }

        final escaped = patterns.collect { Escape.path(it) }
        final targetPath = toUri(targetDir)

        return """
            uploads=()
            IFS=\$'\\n'
            for name in \$(eval "ls -1d ${escaped.join(' ')}" | sort | uniq); do
                uploads+=("nxf_gcs_upload '\$name' '$targetPath'")
            done
            unset IFS
            nxf_parallel "\${uploads[@]}"
        """.stripIndent(true)
    }

    @Override
    String copyFile( String name, Path target ) {
        final targetPath = toUri(target.getParent())
        return "nxf_gcs_upload '${Escape.path(name)}' '$targetPath'"
    }

    @Override
    String fileStr( Path path ) {
        return Escape.path(path.getFileName())
    }

    @Override
    String exitFile(Path path) {
        final targetPath = toUri(path)
        return "| nxf_gcs_upload - '$targetPath' || true"
    }

    @Override
    String pipeInputFile(Path path) {
        return " < ${Escape.path(path.getFileName())}"
    }

    @Override
    String touchFile(Path file) {
        final targetPath = toUri(file)
        return "echo start | nxf_gcs_upload - '$targetPath'"
    }

    @Override
    List<Volume> getVolumes() {
        // when using gcloud for file transfer, we don't need to mount GCS buckets with gcsfuse
        return []
    }

    @Override
    List<String> getContainerMounts() {
        return []
    }

    @Override
    String runCommand() {
        "trap \"{ cp ${TaskRun.CMD_LOG} ${workDir}/${TaskRun.CMD_LOG}; }\" EXIT; /bin/bash ${workDir}/${TaskRun.CMD_RUN} 2>&1 | tee ${TaskRun.CMD_LOG}"
    }

    static protected String toUri(Path path) {
        if( path instanceof CloudStoragePath ) {
            return path.toUri().toString()
        }
        return "gs:///$path"
    }
}
