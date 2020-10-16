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
package org.apache.hadoop.hdfs;

import io.chubao.fs.client.stream.CFSDataInputStream;
import io.chubao.fs.client.util.FsPermissionHelper;
import io.chubao.fs.client.config.CFSConfig;
import io.chubao.fs.sdk.*;
import io.chubao.fs.sdk.exception.CFSException;
import io.chubao.fs.sdk.exception.CFSFileNotFoundException;
import io.chubao.fs.sdk.stream.CFSInputStream;
import io.chubao.fs.sdk.stream.CFSOutputStream;
import io.chubao.fs.client.util.FileStatusHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;

@InterfaceAudience.LimitedPrivate({"MapReduce", "HBase"})
@InterfaceStability.Unstable
public class ChubaoFileSystem extends FileSystem {

  private static final Log log = LogFactory.getLog(ChubaoFileSystem.class);
  private final String CFS_SCHEME_NAME = "cfs";
  private final String CFS_SITE_CONFIG = "cfs-site.xml";

  private URI uri;
  private CFSConfig cfg;
  private CFSClient client;
  private String userHomePrefix;
  private Path workingDir;
  private FileStorage storage;
  private int uid;
  private int gid;

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public void initialize(URI uri, org.apache.hadoop.conf.Configuration config) throws IOException {
    log.info("==> Initialize ChubaoFileSystem.");
    if (!uri.getScheme().equalsIgnoreCase(CFS_SCHEME_NAME)) {
      throw new IllegalArgumentException("Not support the scheme: " + uri.toString() + ", you may be use [cfs://]");
    }
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    super.initialize(uri, config);
    String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
    if (hadoopConfDir == null) {
      hadoopConfDir = System.getProperty("HADOOP_CONF_DIR");
    }

    if (hadoopConfDir == null) {
      throw new IllegalArgumentException("HADOOP_CONF_DIR env or property is not set!");
    }
    String configFile = hadoopConfDir + File.separator + CFS_SITE_CONFIG;
    String userName = System.getProperty("user.name");
    try {
      cfg = new CFSConfig();
      cfg.load(configFile);

      client = new CFSClient(cfg.getCFSlibsdk());
      client.init();
      StorageConfig sconfig = getStorageConfig(cfg);
      storage = client.openFileStorage(sconfig);
      uid = storage.getUid(userName);
      gid = storage.getGidByUser(userName);
      userHomePrefix = cfg.getUserHomePrefix();
      workingDir = getHomeDirectory();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      cfg.setCurrentUser(currentUser.getUserName());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      throw new IOException("Failed to initialize ChubaoFileSystem", e);
    }
  }

  private StorageConfig getStorageConfig(CFSConfig cfg) {
    StorageConfig config = new StorageConfig();
    config.setMasters(cfg.getCFSMasterAddr());
    config.setVolumeName(cfg.getCFSVoumeName());
    config.setOwner(cfg.getCFSVoumeOwner());
    String logDir = cfg.getCFSLogDir();
    if (logDir != null) {
      config.setLogDir(logDir);
    }
    String logLevel = cfg.getCFSLogLevel();
    if (logLevel != null) {
      config.setLogLevel(logLevel);
    }

    config.print();
    return config;
  }

  @Override
  public void close() throws IOException {
    log.info("Close ChubaoFileStem");
    super.close();
  }

  @Override
  public String getScheme() {
    return CFS_SCHEME_NAME;
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(userHomePrefix + "/" + System.getProperty("user.name")));
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("getFileStatus:" + path.toString());
    }
    try {
      String pathStr = parsePath(path);
      CFSStatInfo info = storage.stat(pathStr);
      if (info == null) {
        throw new FileNotFoundException(path.toString());
      }
      return FileStatusHelper.convert(storage, (uri == null ? null : uri.toString()), pathStr, info);
    } catch (CFSException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("list:" + path.toString());
    }
    try {
      FileStatus[] fstatus = null;
      FileStatus status = getFileStatus(path);
      if (status != null && status.isDirectory() == false) {
        fstatus = new FileStatus[1];
        fstatus[0] = status;
        return fstatus;
      }

      String pathStr = parsePath(path);
      CFSStatInfo[] infos = storage.list(pathStr);
      fstatus = new FileStatus[infos.length];
      for (int i = 0; i < infos.length; i++) {
        fstatus[i] = FileStatusHelper.convert(storage, (uri == null ? null : uri.toString()), pathStr, infos[i]);
        log.info("==>" + fstatus[i]);
      }

      return fstatus;
    } catch (CFSFileNotFoundException e) {
      throw new FileNotFoundException(e.getMessage());
    } catch (CFSException ex) {
      throw new IOException(ex);
    }
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return mkdirs(f, FsPermissionHelper.getDirDefault(cfg));
  }

  @Override
  public boolean mkdirs(Path path, FsPermission permission) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Mkdirs: " + path.toString() + " permission: " + permission.toShort());
    }
    statistics.incrementWriteOps(1);
    try {
      /*
      short perm = FsCreateModes.applyUMask(permission,
          FsPermissionHelper.getUMask(getConf())).toShort();

       */
      FsPermission umask = FsPermission.getUMask(getConf());
      short perm = permission.applyUMask(umask).toShort();
      return storage.mkdirs(parsePath(path), perm, uid, gid);
    } catch (Exception e) {
      log.error("Failed to mkdirs:" + path.toString());
      throw new IOException(e);
    }
  }

  @Override
  public FSDataInputStream open(Path path, int bufferSize) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("open:" + path.toString() + " buffsize:" + bufferSize);
    }
    statistics.incrementReadOps(1);
    try {
      CFSFile cfile = storage.open(parsePath(path), FileStorage.O_RDONLY, 0, uid, gid);
      CFSInputStream input = new CFSInputStream(cfile);
      return new FSDataInputStream(new CFSDataInputStream(input));
    } catch (Exception ex) {
      log.error("Failed to open:" + path.toString());
      throw new IOException(ex);
    }
  }

  @Override
  public FSDataOutputStream append(Path path, int bufferSize, Progressable progress) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("append:" + path.toString() + " buffsize:" + bufferSize);
    }
    statistics.incrementWriteOps(1);
    try {
      if (progress != null) {
        progress.progress();
      }
      int flags = FileStorage.O_WRONLY | FileStorage.O_APPEND;
      CFSFile cfile = storage.open(parsePath(path), flags, cfg.CFS_DEFAULT_FILE_PERMISSION, uid, gid);
      CFSOutputStream output = new CFSOutputStream(cfile);
      return new FSDataOutputStream(output, statistics);
    } catch (Exception ex) {
      log.error("Failed to append:" + path.toString());
      throw new IOException(ex);
    }
  }

  @Override
  public FSDataOutputStream create(
      Path f, boolean overwrite, int bufferSize, short replication,
      long blockSize, Progressable progress
  ) throws IOException {
    FsPermission umask = FsPermission.getUMask(getConf());
    FsPermission perm = FsPermissionHelper.getFileDefault(cfg).applyUMask(umask);
    return this.create(f, perm,
        overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  public short getDefaultReplication() {
    return (short) storage.getReplicaNumber();
  }

  @Override
  public FSDataOutputStream create(
      Path path, FsPermission permission, boolean overwrite, int bufferSize,
      short replication, long blockSize, Progressable progress) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Create path: " + path.toString() + " permission: "
          + Integer.toHexString((int) permission.toShort()) + " overwrite: " + overwrite + " bufferSize: " + bufferSize
          + " replication: " + replication + " blockSize: " + blockSize + " progress: " + progress);
    }
    statistics.incrementWriteOps(1);
    CFSFile cfile = null;
    try {
      Path parentPath = path.getParent();
      boolean res = mkdirs(parentPath, permission);
      if (!res) {
        throw new IOException("Failed to mkdirs:" + parentPath.toString());
      }

      String pathStr = parsePath(path);
      if (exists(path)) {
        if (overwrite) {
          cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_TRUNC, permission.toShort(), uid, gid);
        } else {
          CFSStatInfo stat = storage.stat(pathStr);
          if (stat != null) {
            throw new FileAlreadyExistsException(pathStr);
          }
          cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_CREAT, permission.toShort(), uid, gid);
        }
      } else {
        cfile = storage.open(pathStr, FileStorage.O_WRONLY | FileStorage.O_CREAT, permission.toShort(), uid, gid);
      }

      if (progress != null) {
        progress.progress();
      }

      CFSOutputStream output = new CFSOutputStream(cfile);
      return new FSDataOutputStream(output, statistics);
    } catch (CFSFileNotFoundException e) {
      throw new FileNotFoundException(e.getMessage());
    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public boolean truncate(Path path, long newLength) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("truncate:" + path.toString() + " newlength:" + newLength);
    }
    statistics.incrementWriteOps(1);

    FileStatus status = getFileStatus(path);
    if (status == null || status.isDirectory() || status.getLen() <= newLength) {
      return false;
    }

    try {
      storage.truncate(parsePath(path), newLength);
    } catch (Exception ex) {
      log.error("Failed to truncate:" + path.toString());
      throw new IOException(ex);
    }
    return true;
  }

  @Override
  public boolean delete(Path path, boolean recursive) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("delete:" + path.toString() + " recursive:" + recursive);
    }
    statistics.incrementWriteOps(1);

    String str = null;
    try {
      str = parsePath(path);
      CFSStatInfo info = storage.stat(str);
      if (info == null) {
        return false;
        //throw new FileNotFoundException(path.toString());
      }

      if (info.getType() == CFSStatInfo.Type.DIR) {
        storage.rmdir(str, recursive);
      } else if (info.getType() == CFSStatInfo.Type.REG || info.getType() == CFSStatInfo.Type.LINK) {
        storage.unlink(str);
      } else {
        throw new IOException("Not support the type:" + info.getType());
      }
    } catch (Exception ex) {
      log.error("Failed to delete:" + path.toString());
      throw new IOException(ex);
    }
    return true;
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("rename:" + src.toString() + " to:" + dst.toString());
    }
    statistics.incrementWriteOps(1);
    String from = null;
    String to = null;
    /*
    If src and dst are file, and are exists.
    1. src and dst are same a file, return true.
       the case in testMoveFileUnderParent
    2. Or not, return false.
       the case in testRenameFileAsExistingFile
     */
    if (LOG.isDebugEnabled()) {
      LOG.debug("RenameEntry:" + src + " to:" + dst);
    }
    try {
      from = parsePath(src);
      to = parsePath(dst);
      CFSStatInfo srcInfo = storage.stat(from);
      if (srcInfo == null) {
        return false;
      }
      if (srcInfo.getType() == CFSStatInfo.Type.REG) {
        CFSStatInfo dstInfo = storage.stat(to);
        if (dstInfo != null && dstInfo.getType() == CFSStatInfo.Type.REG) {
          if (src == dst) {
            return true;
          }
          return false;
        }
      }
    } catch (CFSException ex) {
      log.error("Failed to rename:" + src + " to:" + dst);
      throw new IOException(ex);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Rename:" + from + " to:" + to);
    }
    try {
      storage.rename(from, to);
      return true;
    } catch (CFSException ex) {
      log.error("Failed to rename:" + src + " to:" + dst);
      log.error(ex.getMessage(), ex);
      return false;
    }
  }

  @Override
  public void setWorkingDirectory(Path dir) {
    this.workingDir = fixRelativePart(dir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public FsStatus getStatus() throws IOException {
    throw new IOException("Not implement getStatus.");
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path path, FsPermission permission, EnumSet<CreateFlag> flags,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    if (!flags.contains(CreateFlag.CREATE)) {
      throw new IOException("Not support the flags:" + flags.toString());
    }

    String pathStr = parsePath(path);
    CFSStatInfo info = null;
    try {
      info = storage.stat(pathStr);
    } catch (CFSException e) {
      throw new IOException(e);
    }

    int fls = FileStorage.O_WRONLY;
    if (flags.size() == 1) {
      fls = fls | FileStorage.O_CREAT;
    } else if ((flags.size() == 2) && (flags.contains(CreateFlag.OVERWRITE))) {
      if (info == null) {
        fls = FileStorage.O_WRONLY | FileStorage.O_CREAT;
      } else if (info.getType() != CFSStatInfo.Type.DIR) {
        fls = FileStorage.O_WRONLY | FileStorage.O_TRUNC;
      } else {
        throw new IOException("The path: " + path.toString() + " is a dir.");
      }
    } else {
      throw new IOException("Invalid flags:" + flags.toString());
    }

    statistics.incrementWriteOps(1);
    CFSFile cfile = null;
    try {
      cfile = storage.open(pathStr, fls, cfg.CFS_DEFAULT_FILE_PERMISSION, uid, gid);
    } catch (CFSException ex) {
      log.error("Failed to create:" + path.toString());
      throw new IOException(ex);
    }
    CFSOutputStream output = new CFSOutputStream(cfile);
    return new FSDataOutputStream(output, statistics);
  }

  @Override
  public void setPermission(Path path, FsPermission permission) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("setPermission:" + path.toString() + " permission:" + permission);
    }
    try {
      storage.chmod(parsePath(path), permission.toShort());
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public void setOwner(Path path, String username, String groupname) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("setOwner:" + path.toString() + " username:" + username + " groupname:" + groupname);
    }
    try {
      storage.chown(parsePath(path), username, groupname);
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public void setTimes(Path path, long mtime, long atime) throws IOException {
    if (mtime < 0) {
      mtime = 0L;
    }
    if (atime < 0) {
      atime = 0L;
    }

    if (mtime > 0) {
      mtime = mtime * 1000;
    }

    if (atime > 0) {
      atime = atime * 1000;
    }

    if (log.isDebugEnabled()) {
      log.debug("setTimes:" + path.toString() + " mtime:" + mtime + " atime:" + atime);
    }
    try {
      storage.setTimes(parsePath(path), mtime, atime);
    } catch (CFSException ex) {
      log.error(ex.getMessage(), ex);
      throw new IOException(ex);
    }
  }

  @Override
  public FileChecksum getFileChecksum(Path file, long length) throws IOException {
    throw new IOException("Unimplement getFileCheckSum:  " + file.toString() + ".");
  }

  @Override
  public AclStatus getAclStatus(Path path) throws IOException {
    throw new IOException("Unsupport getAclStatus: " + path.toString() + ".");
  }

  @Override
  public void setAcl(Path path, List<AclEntry> aclSpec) throws IOException {
    throw new IOException("Unsupport setAcl: " + path.toString() + ".");
  }

  @Override
  public void removeAclEntries(Path path, List<AclEntry> aclSpec) throws IOException {
    throw new IOException("Unsupport removeAclEntryes: " + path.toString() + ".");
  }

  @Override
  public byte[] getXAttr(Path path, String name) throws IOException {
    throw new IOException("Not implement getXAttr:  " + path.toString() + ".");
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path) throws IOException {
    throw new IOException("Not implement getXAttrs:  " + path.toString() + ".");
  }

  @Override
  public Map<String, byte[]> getXAttrs(Path path, List<String> names) throws IOException {
    throw new IOException("Not implement getXAttrs:  " + path.toString() + ".");
  }

  @Override
  public List<String> listXAttrs(Path path) throws IOException {
    throw new IOException("Not implement listXAttrs:  " + path.toString() + ".");
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value) throws IOException {
    throw new IOException("Not implement setXAttrs:  " + path.toString() + ".");
  }

  @Override
  public void setXAttr(Path path, String name, byte[] value, EnumSet<XAttrSetFlag> flag) throws IOException {
    throw new IOException("Not implement setXAttrs:  " + path.toString() + ".");
  }

  @Override
  public void removeXAttr(Path path, String name) throws IOException {
    throw new IOException("Not implement removeXAttrs:  " + path.toString() + ".");
  }

  private String parsePath(Path p) {
    /*
    if (log.isDebugEnabled()) {
      log.debug("path:" + p.toString());
    }
     */
    Path path = makeQualified(p);
    URI pathURI = path.toUri();
    if (pathURI == null) {
      throw new IllegalArgumentException("Couldn't parse the path, " + path);
    }

    String res = null;
    String scheme = pathURI.getScheme();
    if (scheme != null) {
      if (scheme.equals(CFS_SCHEME_NAME) == false) {
        throw new IllegalArgumentException("Not support the scheme, from the path, " + path);
      }

      res = pathURI.getPath().trim();
      if (res == null || res.isEmpty()) {
        throw new IllegalArgumentException(path + " is invalid.");
      }
    } else {
      res = pathURI.toString().trim();
      if (res == null || res.isEmpty()) {
        log.warn("Reset the path with working-dir: " + workingDir);
        res = workingDir.toString();
      } else if (path.isAbsolute() == false) {
        res = new Path(workingDir.toString() + path.toString()).toString();
      } else {
        res = path.toString();
      }
    }

    if (res.startsWith("/") == false) {
      throw new IllegalArgumentException(path + " is invalid.");
    }

    return res;
  }

  private String validPath(Path path) {
    log.info("path:" + path.toString());
    URI pathURI = path.toUri();
    log.info("URI.getPath:" + pathURI.getPath());
    if (pathURI == null) {
      throw new IllegalArgumentException("Couldn't parse the path, " + path);
    }

    String res = null;
    String scheme = pathURI.getScheme();
    if (scheme != null) {
      if (scheme.equals(CFS_SCHEME_NAME) == false) {
        throw new IllegalArgumentException("Not support the scheme, from the path, " + path);
      }

      res = pathURI.getPath().trim();
      if (res == null || res.isEmpty()) {
        throw new IllegalArgumentException(path + " is invalid.");
      }
    } else {
      res = pathURI.toString().trim();
      if (res == null || res.isEmpty()) {
        log.warn("Reset the path with working-dir: " + workingDir);
        res = workingDir.toString();
      } else if (path.isAbsolute() == false) {
        res = new Path(workingDir.toString() + path.toString()).toString();
      } else {
        res = path.toString();
      }
    }

    if (res.startsWith("/") == false) {
      throw new IllegalArgumentException(path + " is invalid.");
    }

    return res;
  }
}
