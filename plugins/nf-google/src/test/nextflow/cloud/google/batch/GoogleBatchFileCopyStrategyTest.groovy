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

import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper
import nextflow.cloud.google.GoogleOpts
import nextflow.processor.TaskBean
import spock.lang.Specification

import java.nio.file.Path

class GoogleBatchFileCopyStrategyTest extends Specification {

    def 'should return stage input input file'() {
        given:
        def fs = LocalStorageHelper.getOptions().getService().getStorage()
        fs.create(com.google.cloud.storage.BlobInfo.newBuilder('my-bucket', 'foo.txt').build())
        Path file = fs.get('my-bucket').resolve('foo.txt')

        def bean = Mock(TaskBean)
        def opts = new GoogleOpts([:])
        def copy = new GoogleBatchFileCopyStrategy(bean, opts)

        when:
        def script = copy.stageInputFile( file, 'bar.txt')
        then:
        script == "downloads+=("nxf_gcs_download 'gs://my-bucket/foo.txt' 'bar.txt'")" as String
    }

    def 'should return unstage script' () {
        given:
        def copy = new GoogleBatchFileCopyStrategy(Mock(TaskBean), new GoogleOpts([:]))
        def target = LocalStorageHelper.getOptions().getService().getStorage().get('my-bucket').resolve('results')

        when:
        def script = copy.getUnstageOutputFilesScript(['file.txt'],target)
        then:
        script.trim() == '''
                    uploads=()
                    IFS=$'
'
                    for name in $(eval "ls -1d file.txt" | sort | uniq); do
                        uploads+=("nxf_gcs_upload '$name' 'gs://my-bucket/results'")
                    done
                    unset IFS
                    nxf_parallel "${uploads[@]}"
                    '''
                    .stripIndent().trim()

    }

    def 'should check the beforeScript' () {

        given:
        def bean = Mock(TaskBean)
        def opts = new GoogleOpts([:])
        def copy = new GoogleBatchFileCopyStrategy(bean, opts)

        when:
        def script = copy.getBeforeStartScript()
        then:
        script.contains('function nxf_gcs_download()')
        script.contains('function nxf_gcs_upload()')
        script.contains('if ! command -v gcloud &> /dev/null')
    }
}
