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
package io.chubao.fs.client.util;

import io.chubao.fs.sdk.CFSStatInfo;
import io.chubao.fs.sdk.FileStorage;
import io.chubao.fs.sdk.exception.CFSException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

public class FileStatusHelper {
  private static final Log log = LogFactory.getLog(FileStatusHelper.class);

  public static FileStatus convert(FileStorage storage, String uri, String path, CFSStatInfo info) throws CFSException {
    boolean isDir = info.getType() == CFSStatInfo.Type.DIR;
    Path p = null;
    if (info.getName() == null || info.getName().isEmpty()) {
      if (uri != null) {
        p = new Path(uri + path);
      } else {
        p = new Path(path);
      }
    } else {
      if (uri != null) {
        p = new Path(uri + path + "/" + info.getName());
      } else {
        p = new Path(path + "/" + info.getName());
      }
    }
    return new FileStatus(info.getSize(),
        isDir,
        storage.getReplicaNumber(),
        storage.getBlockSize(),
        info.getMtime() * 1000,
        info.getAtime() * 1000,
        new FsPermission((short) info.getMode()),
        storage.getUser(info.getUid()),
        storage.getGroup(info.getGid()),
        p);
  }

  public static FileStatus convert(FileStorage storage, Path path, CFSStatInfo info) throws CFSException {
    boolean isDir = info.getType() == CFSStatInfo.Type.DIR;
    return new FileStatus(info.getSize(),
        isDir,
        storage.getReplicaNumber(),
        storage.getBlockSize(),
        info.getMtime(),
        info.getAtime(),
        new FsPermission((short) info.getMode()),
        storage.getUser(info.getUid()),
        storage.getGroup(info.getGid()),
        path);
  }
}
