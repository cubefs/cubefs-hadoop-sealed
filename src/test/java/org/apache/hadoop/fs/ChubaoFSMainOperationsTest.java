// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package org.apache.hadoop.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ChubaoFSMainOperationsTest extends FSMainOperationsBaseTest {
  private static final Log log = LogFactory.getLog(ChubaoFSMainOperationsTest.class);

  @Override
  protected FileSystem createFileSystem() throws Exception {
    log.info("Create ChubaoFileSystem");
    Configuration conf = new Configuration();
    String cfsconf = System.getenv("HADOOP_CONF_DIR");
    System.setProperty("HADOOP_CONF_DIR", cfsconf);
    return FileSystem.get(conf);
  }

  @Test
  public void testGlobStatusThrowsExceptionForUnreadableDir()
      throws Exception {
    log.warn("Not support the ACL, so skip the test.");
  }

  @Test
  public void testListStatusThrowsExceptionForUnreadableDir()
      throws Exception {
    log.warn("Not support the ACL, so skip the test.");
  }
}
