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

  private Map<String, Map<String, Bitmap>> bitmaps = new HashMap<String, Map<String, Bitmap>>();

  private static UserPropertyBitmaps instance = new UserPropertyBitmaps();

  public static UserPropertyBitmaps getInstance() {
    return instance;
  }

  private UserPropertyBitmaps() {

    permittedProjects.add("citylife");
    permittedProjects.add("happyranch");
    permittedProjects.add("happyfarmer");
    permittedProjects.add("ddt");
    permittedProjects.add("sof-dsk");
    permittedProjects.add("sof-newgdp");


    for (String projectID : permittedProjects) {
      bitmaps.put(projectID, new HashMap<String, Bitmap>());
    }


    for (String ref : User.refFields) {
      permittedProperties.add(ref);
    }
    permittedProperties.add(User.registerField);
    permittedProperties.add(User.lastLoginTimeField);
    permittedProperties.add(User.firstPayTimeField);
    permittedProperties.add(User.nationField);
    permittedProperties.add(User.refField);
    permittedProperties.add(User.ref0Field);
    permittedProperties.add(User.ref1Field);
    permittedProperties.add(User.ref2Field);
    permittedProperties.add(User.ref3Field);
    permittedProperties.add(User.ref4Field);
    permittedProperties.add(User.geoipField);


  }

  public boolean isPropertyHit(String projectID, long userID, String propertyName) {
    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return false;
    }
    Bitmap bitmap = project.get(propertyName);
    if (bitmap == null) {
      if (permittedProperties.contains(propertyName)) {
        bitmap = new Bitmap();
        project.put(propertyName, bitmap);
      } else {
        return false;
      }
    }
    return bitmap.get(userID);

  }

  public void markPropertyHit(String projectID, long userID, String propertyName) {
    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return;
    }
    Bitmap bitmap = project.get(propertyName);
    if (bitmap == null) {
      if (permittedProperties.contains(propertyName)) {
        bitmap = new Bitmap();
        project.put(propertyName, bitmap);
      } else {
        return;
      }
    }
    bitmap.set(userID, true);
  }

  public void resetPropertyMap(String projectID, String propertyName) {
    Map<String, Bitmap> project = bitmaps.get(projectID);
    if (project == null) {
      return;
    }
    Bitmap bitmap = project.get(propertyName);
    if (bitmap == null) {
      if (permittedProperties.contains(propertyName)) {
        bitmap = new Bitmap();
        project.put(propertyName, bitmap);
      } else {
        return;
      }
    }
    bitmap.reset();
  }

  public static void main(String[] args) {
    getInstance().isPropertyHit("test", 134253123, "ref");

  }

}
