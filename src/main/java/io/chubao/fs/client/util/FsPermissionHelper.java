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

import io.chubao.fs.client.config.CFSConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.permission.FsPermission;

public class FsPermissionHelper {
  public static FsPermission getUMask(Configuration conf) {
    String maskStr = conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    return new FsPermission(maskStr);
  }

  public static FsPermission getDirDefault(CFSConfig config) {
    return new FsPermission(config.getDirDefaultPermission());
  }

  public static FsPermission getFileDefault(CFSConfig config) {
    return new FsPermission(config.getFileDefaultPermission());
  }

}
