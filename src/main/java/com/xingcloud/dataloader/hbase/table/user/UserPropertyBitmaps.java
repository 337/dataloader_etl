package com.xingcloud.dataloader.hbase.table.user;

import com.xingcloud.dataloader.lib.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Author: mulisen
 * Date:   10/31/12
 */
public class UserPropertyBitmaps {

  static final Set<String> permittedProjects = new HashSet<String>();

  static final Set<String> permittedProperties = new HashSet<String>();

  static final Set<String> specialProperties = new HashSet<String>();

  private Map<String, Map<String, Bitmap>> bitmaps = new HashMap<String, Map<String, Bitmap>>();

  private static UserPropertyBitmaps instance = new UserPropertyBitmaps();

  public static UserPropertyBitmaps getInstance() {
    return instance;
  }

  private UserPropertyBitmaps() {

    permittedProjects.add("citylife");
    permittedProjects.add("happyranch");
    permittedProjects.add("happyfarmer");
    permittedProjects.add("v9-sof");
    permittedProjects.add("ddt");
    permittedProjects.add("sof-dsk");
    permittedProjects.add("sof-newgdp");

    permittedProperties.add(User.registerField);
    permittedProperties.add(User.lastLoginTimeField);
    permittedProperties.add(User.firstPayTimeField);
    permittedProperties.add(User.nationField);
    permittedProperties.add(User.geoipField);

    for (String ref : User.refFields)
      specialProperties.add(ref);

  }

  public boolean isPropertyHit(String projectID, long userID, String propertyName) {

    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return false;
    }
    String propertyNameIndeed = transferpropertyName(propertyName);

    Bitmap bitmap = project.get(propertyNameIndeed);
    return bitmap != null && bitmap.get(userID);

  }

  public String transferpropertyName(String propertyName) {
    String propertyNameIndeed = null;
    if (specialProperties.contains(propertyName))
      propertyNameIndeed = User.refField;
    else
      propertyNameIndeed = propertyName;
    return propertyNameIndeed;
  }

  public void markPropertyHit(String projectID, long userID, String propertyName) {
    if (specialProperties.contains(propertyName) || (permittedProjects.contains(projectID) && permittedProperties
            .contains(propertyName))) {
      Map<String, Bitmap> project = bitmaps.get(projectID);
      if (project == null) {
        project = new HashMap<String, Bitmap>();
        bitmaps.put(projectID, project);
      }
      String propertyNameIndeed = transferpropertyName(propertyName);
      Bitmap bitmap = project.get(propertyNameIndeed);
      if (bitmap == null) {
        bitmap = new Bitmap();
        project.put(propertyNameIndeed, bitmap);
      }
      bitmap.set(userID, true);
    }
  }

  public void resetPropertyMap(String projectID, String propertyName) {
    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return;
    }
    String propertyNameIndeed = transferpropertyName(propertyName);
    Bitmap bitmap = project.get(propertyNameIndeed);
    if (bitmap == null) {
      return;
    }
    bitmap.reset();
  }

  public boolean ifPropertyNull(String projectID,String propertyName){
    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return true;
    }
    String propertyNameIndeed = transferpropertyName(propertyName);
    Bitmap bitmap = project.get(propertyNameIndeed);
    return bitmap == null;
  }
}
