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

import io.chubao.fs.client.config.CFSConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.ChubaoFileSystem;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;

public class ChubaoAbstractFS extends AbstractFileSystem {

  private static final Log log = LogFactory.getLog(ChubaoAbstractFS.class);

  private ChubaoFileSystem cfs;
  private boolean verifyChecksum = false;
  private FsServerDefaults fsServer;

  public ChubaoAbstractFS(final URI theUri, final Configuration conf) throws IOException, URISyntaxException {
    super(theUri, CFSConfig.CFS_SCHEME_NAME, true, CFSConfig.CFS_DEFAULT_PORT);
    log.info("==> Initialize ChubaoAbstractFS.");

    if (!theUri.getScheme().equalsIgnoreCase(CFSConfig.CFS_SCHEME_NAME)) {
      throw new IllegalArgumentException(theUri.toString() + " is not ChubaoFS scheme, Unable to initialize ChubaoFS");
    }

    String host = theUri.getHost();
    if (host == null) {
      throw new IOException("Incomplete ChubaoFS URI, no host: " + theUri);
    }

    cfs = new ChubaoFileSystem();

    cfs.initialize(theUri, conf);

    fsServer = new FsServerDefaults(
        64 * 1024 * 1024,
        1,
        64 * 1024 * 1024,
        (short) 3,
        64 * 1024 * 1024,
        false,
        3600,
        org.apache.hadoop.util.DataChecksum.Type.NULL);
  }

  @Override
  public int getUriDefaultPort() {
    return CFSConfig.CFS_DEFAULT_PORT;
  }

  protected void finalize() throws Throwable {
    try {
      cfs.close();
    } finally {
      super.finalize();
    }
  }

  @Override
  public boolean truncate(Path f, long newLength) throws AccessControlException, FileNotFoundException, UnresolvedLinkException, IOException {
    return cfs.truncate(f, newLength);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    return cfs.getFileStatus(path);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    return cfs.listStatus(path);
  }

  @Override
  public boolean supportsSymlinks() {
    return true;
  }


  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    return cfs.open(path, bufferSize);
  }

  @Override
  public FSDataOutputStream createInternal(
      Path f,
      EnumSet<CreateFlag> flag,
      FsPermission absolutePermission,
      int bufferSize,
      short replication,
      long blockSize,
      Progressable progress,
      ChecksumOpt checksumOpt,
      boolean createParent)
      throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException,
      ParentNotDirectoryException,
      UnsupportedFileSystemException,
      UnresolvedLinkException,
      IOException {
    if (createParent) {
      try {
        cfs.mkdirs(f.getParent(), absolutePermission);
      } catch (IOException e) {
        throw e;
      }
    }
    return cfs.create(f, absolutePermission, flag, bufferSize, replication, blockSize, progress, checksumOpt);
  }

  @Override
  public void mkdir(Path dir, FsPermission permission, boolean createParent)
      throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException,
      UnresolvedLinkException,
      IOException {
    if (log.isDebugEnabled()) {
      log.debug("mkdir: " + dir.toString());
    }
    if (createParent) {
      try {
        cfs.mkdirs(dir.getParent(), permission);
      } catch (IOException e) {
        throw e;
      }
    }
    boolean result = cfs.mkdirs(dir, permission);
    if (!result) {
      throw new IOException("Failed to mkdir: " + dir.toString());
    }
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    return cfs.delete(path, recursive);
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len) throws IOException {
    return cfs.getFileBlockLocations(f, start, len);
  }

  @Override
  public FileChecksum getFileChecksum(Path f) throws IOException {
    return cfs.getFileChecksum(f, Long.MAX_VALUE);
  }

  @Override
  public FsStatus getFsStatus() throws IOException {
    return cfs.getStatus();
  }

  @Override
  public FsServerDefaults getServerDefaults() throws IOException {
    return fsServer;
  }

  @Override
  public void renameInternal(Path src, Path dst)
      throws AccessControlException,
      FileAlreadyExistsException,
      FileNotFoundException,
      ParentNotDirectoryException,
      IOException,
      UnresolvedLinkException {
    if (cfs.rename(src, dst) == false) {
      throw new IOException("Failed to rename:" + src.toString() + " to:" + dst.toString());
    }
  }

  @Override
  public void setOwner(Path f, String username, String groupname) throws IOException {
    cfs.setOwner(f, username, groupname);
  }

  @Override
  public void setPermission(Path f, FsPermission permission) throws IOException {
    cfs.setPermission(f, permission);
  }

  @Override
  public boolean setReplication(Path f, short replication) throws IOException {
    return false;
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    cfs.setTimes(f, mtime, atime);
  }

  @Override
  public void setVerifyChecksum(boolean verifyChecksum) throws IOException {
    this.verifyChecksum = verifyChecksum;
  }
}
