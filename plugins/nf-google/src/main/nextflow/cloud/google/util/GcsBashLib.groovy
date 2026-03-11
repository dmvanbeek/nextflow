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
package nextflow.cloud.google.util

import groovy.transform.CompileStatic
import nextflow.util.Duration

/**
 * Provides a bash script function to handle files from/to Google cloud storage
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@CompileStatic
class GcsBashLib {

    static String script(int maxParallelTransfers, int maxTransferAttempts, Duration delayBetweenAttempts) {
        """
        if ! command -v gcloud &> /dev/null
        then
            echo "gcloud command could not be found, but it is required for 'gcloud' file handling mode. Please install it in your container image." >&2
            exit 1
        fi

        function nxf_gcs_download() {
            local src_path="\$1"
            local dst_path="\$2"
            local dst_path_esc=\$(nxf_escape_path "\$dst_path")
            local retry_attempts=$maxTransferAttempts
            local retry_delay_secs=${delayBetweenAttempts?.toSeconds() ?: 2}
            
            # Create the destination directory if it doesn't exist
            if [[ ! -d \$(dirname "\$dst_path_esc") ]]; then
              mkdir -p \$(dirname "\$dst_path_esc")
            fi

            for ((i=1; i<=\$retry_attempts; i++)); do
                gcloud storage cp "\$src_path" "\$dst_path_esc"
                local exit_code=\$?
                if [ \$exit_code -eq 0 ]; then
                    return 0
                fi
                echo "WARNING: Failed to download '\$src_path' with exit code \$exit_code. Attempt \$i of \$retry_attempts. Retrying in \$retry_delay_secs seconds..." >&2
                sleep \$retry_delay_secs
            done

            echo "ERROR: Failed to download '\$src_path' after \$retry_attempts attempts." >&2
            return 1
        }

        function nxf_gcs_upload() {
            local src_path="\$1"
            local dst_path="\$2"
            local src_path_esc=\$(nxf_escape_path "\$src_path")
            local retry_attempts=$maxTransferAttempts
            local retry_delay_secs=${delayBetweenAttempts?.toSeconds() ?: 2}

            for ((i=1; i<=\$retry_attempts; i++)); do
                gcloud storage cp "\$src_path_esc" "\$dst_path"
                local exit_code=\$?
                if [ \$exit_code -eq 0 ]; then
                    return 0
                fi
                echo "WARNING: Failed to upload '\$src_path_esc' with exit code \$exit_code. Attempt \$i of \$retry_attempts. Retrying in \$retry_delay_secs seconds..." >&2
                sleep \$retry_delay_secs
            done

            echo "ERROR: Failed to upload '\$src_path_esc' after \$retry_attempts attempts." >&2
            return 1
        }

        # Function to escape paths for shell commands
        function nxf_escape_path() {
          echo "\$1" | sed "s/'/'\\\\''/g"
        }
        
        # A poor man parallel command launcher
        function nxf_parallel() {
            local tasks=(\$*)
            for task in "\${tasks[@]}"; do
                eval "\$task" &
            done
            wait
        }
        """.stripIndent()
    }
}
