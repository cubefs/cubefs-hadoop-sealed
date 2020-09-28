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
package io.chubao.fs.client.config;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.HashMap;
import java.util.Map;


public class CFSConfig implements Serializable {
  public final static String CFS_SCHEME_NAME = "cfs";
  public final static int CFS_DEFAULT_PORT = 8888;
  public final short CFS_DEFAULT_UMASK = 0022;
  public final short CFS_DEFAULT_FILE_PERMISSION = 0644;
  public final short CFS_DEFAULT_DIR_PERMISSION = 0644;

  private final String CFS_MASTER_ADDR_KEY = "cfs.master.addr";
  private final String CFS_VOLUME_NAME_KEY = "cfs.volume.name";
  private final String CFS_VOLUME_OWNER_KEY = "cfs.volume.owner";

  private final String CFS_LOG_DIR_KEY = "cfs.log.dir";
  private final String CFS_LOG_LEVEL_KEY = "cfs.log.level";

  private final String CFS_FOLLOWER_READ_KEY = "cfs.follower.read";
  private final boolean CFS_FOLLOWER_READ_DEFAULT = false;

  private final String CFS_LIBSDK_PATH_KEY = "cfs.libsdk.path";

  private final String CFS_USER_HOME_DIR_PREFIX_KEY = "dfs.user.home.dir.prefix";
  private final String CFS_USER_HOME_DIR_PREFIX_DEFAULT = "/user";

  private final String CFS_CURRENT_USE_KEY = "cfs.current.user.name";

  private Map<String, String> configs = new HashMap<>();

  public CFSConfig() {
  }

  public String getCFSMasterAddr() {
    return configs.get(CFS_MASTER_ADDR_KEY);
  }

  public String getCFSVoumeName() {
    return configs.get(CFS_VOLUME_NAME_KEY);
  }

  public String getCFSVoumeOwner() {
    return configs.get(CFS_VOLUME_OWNER_KEY);
  }

  public String getCFSLogDir() {
    return configs.get(CFS_LOG_DIR_KEY);
  }

  public String getCFSLogLevel() {
    return configs.get(CFS_LOG_LEVEL_KEY);
  }

  public boolean getCFSFollowerRead() {
    String res = configs.get(CFS_FOLLOWER_READ_KEY);
    if (res == null) {
      return CFS_FOLLOWER_READ_DEFAULT;
    }
    if (res.equals("true")) {
      return true;
    } else {
      return false;
    }
  }

  public String getCFSlibsdk() {
    return configs.getOrDefault(CFS_LIBSDK_PATH_KEY, null);
  }

  public int getLRUCache() {
    return 100;
  }

  public String getChemeName() {
    return CFS_SCHEME_NAME;
  }

  public String getUserHomePrefix() {
    return configs.getOrDefault(CFS_USER_HOME_DIR_PREFIX_KEY, CFS_USER_HOME_DIR_PREFIX_DEFAULT);
  }

  public short getUMask() {
    return CFS_DEFAULT_UMASK;
  }

  public short getFileDefaultPermission() {
    return CFS_DEFAULT_FILE_PERMISSION;
  }

  public short getDirDefaultPermission() {
    return CFS_DEFAULT_DIR_PERMISSION;
  }

  public String getCurrentUser() {
    return configs.get(CFS_CURRENT_USE_KEY);
  }

  public void setCurrentUser(String userName) {
    configs.put(CFS_CURRENT_USE_KEY, userName);
  }

  public void load(String configFilie) throws Exception {
    try {
      parse(configFilie);
    } catch (Exception ex) {
      throw new Exception("Failed to parse the config, " + configFilie, ex);
    }

    boolean checkRes = check();
    if (checkRes == false) {
      throw new Exception("There is may be less some config.");
    }
  }

  private void parse(String configFile) throws FileNotFoundException, XMLStreamException {
    XMLInputFactory factory = XMLInputFactory.newInstance();
    Reader fileReader = new FileReader(new File(configFile));
    factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    XMLStreamReader reader = factory.createXMLStreamReader(fileReader);
    Map<String, String> map = new HashMap();
    try {
      int event = reader.getEventType();
      String name = null;
      String value = "";
      while (true) {
        switch (event) {
          case XMLStreamConstants.START_ELEMENT:
            String localName = reader.getLocalName();
            switch (localName) {
              case "property":
                name = null;
                value = "";
                break;
              case "name":
                name = reader.getElementText();
                break;
              case "value":
                value = reader.getElementText();
            }
            break;
          case XMLStreamConstants.CHARACTERS:
            break;
          case XMLStreamConstants.END_ELEMENT:
            localName = reader.getLocalName();
            switch (localName) {
              case "property":
                if (name != null)
                  map.put(name, value);
                break;
            }
            break;
        }
        if (!reader.hasNext()) {
          break;
        }
        event = reader.next();
      }
    } finally {
      reader.close();
    }
    this.configs = map;
  }

  private boolean check() {
    if (getCFSMasterAddr() == null) {
      return false;
    }

    if (getCFSVoumeName() == null) {
      return false;
    }

    if (getCFSVoumeOwner() == null) {
      return false;
    }

    return true;
  }
}
