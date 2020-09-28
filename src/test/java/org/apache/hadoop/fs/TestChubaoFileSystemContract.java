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
import org.apache.hadoop.hdfs.ChubaoFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class TestChubaoFileSystemContract extends FileSystemContractBaseTest {
  private static final Log log = LogFactory.getLog(TestChubaoFileSystemContract.class);

  @Before
  public void setUp() throws IOException {
    Configuration conf = new Configuration();
    String cfsconf = System.getenv("HADOOP_CONF_DIR");
    System.setProperty("HADOOP_CONF_DIR", cfsconf);
    fs = FileSystem.get(conf);
    System.out.println("fs:" + fs.getScheme());
  }

  @Test
  public void testFsStatus() throws Exception {
    log.warn("Not implement the function.");
  }

  @Test
  public void testListStatusForFile() throws Exception {
    Path basePath = getTestBaseDir();
    String pathStr = basePath.toString() + "/testListStatusForFile" + "/file0";
    Path path = new Path(pathStr);
    fs.create(path);
    Assert.assertTrue(fs.exists(path));
    FileStatus[] status = fs.listStatus(path);
    Assert.assertEquals(status.length, 1);
  }

  @Test
  public void testReadByBuffer() throws Exception {
    Path basePath = getTestBaseDir();
    String pathStr = basePath.toString() + "/testReadByBuffer" + "/file0";
    Path path = new Path(pathStr);
    FSDataOutputStream out = fs.create(path);
    int dataSize = 1024 * 1024 * 10;
    byte[] data = new byte[dataSize];
    byte[] block = "01234567".getBytes();
    for (int i = 0; i < dataSize; i++) {
      data[i] = block[i % 8];
    }
    out.write(data);
    out.close();

    FSDataInputStream in = fs.open(path);
    ByteBuffer buff = ByteBuffer.allocate(1024);
    int readSize = 0;
    int len = 0;
    while (true) {
      readSize = in.read(buff);
      if (readSize == -1) {
        break;
      }
      for (int i = 0; i < readSize; i++) {

        Assert.assertEquals(data[len + i], buff.get(i));
      }
      buff.clear();
      len += readSize;
    }
    Assert.assertEquals(len, dataSize);
    in.close();
  }


  /*
  @After
  public void tearDown() throws Exception {

  }
   */
}
