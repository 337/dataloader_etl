package com.xingcloud.dataloader.hbase.table.user;

import com.xingcloud.dataloader.lib.User;
import org.junit.Assert;
import org.junit.Test;

/**
 * User: IvyTang
 * Date: 13-7-15
 * Time: PM6:34
 */
public class UserPropertyBitmapsTest {

  /*
  如果pid不在permitted里面，除了special的属性，其他属性都不在缓存中
   */
  @Test
  public void testIllegalPid() {
    String pid = "test";

    //permitted的一般属性，不能被缓存
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123, User.nationField));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 123, User.nationField);
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123, User.nationField));
    Assert.assertTrue(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,User.nationField));

    //既非permitted属性，也不是special属性，不能被缓存
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123, User.languageField));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 123, User.languageField);
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123, User.languageField));
    Assert.assertTrue(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,User.languageField));

    //special属性，被缓存，且更新了ref*的属性后，其他的ref相关属性都被mark
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123, User.ref3Field));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 123, User.ref4Field);
    for(String ref:User.refFields){
      Assert.assertTrue(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 123,ref));
      Assert.assertFalse(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,ref));
    }

  }

  @Test
  public void testLegalPid(){
    String pid= "sof-dsk";
    //既非permitted属性，也不是special属性，不能被缓存
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213, User.languageField));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 213, User.languageField);
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213, User.languageField));
    Assert.assertTrue(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,User.nationField));


    //permitted的一般属性，被缓存
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213, User.nationField));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 213, User.nationField);
    Assert.assertTrue(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213, User.nationField));
    Assert.assertFalse(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,User.nationField));


    //special属性，被缓存，且更新了ref*的属性后，其他的ref相关属性都被mark
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213, User.ref3Field));
    Assert.assertFalse(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 222, User.refField));
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 213, User.ref4Field);
    UserPropertyBitmaps.getInstance().markPropertyHit(pid, 222, User.refField);
    for(String ref:User.refFields){
      Assert.assertTrue(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 213,ref));
      Assert.assertTrue(UserPropertyBitmaps.getInstance().isPropertyHit(pid, 222,ref));
      Assert.assertFalse(UserPropertyBitmaps.getInstance().ifPropertyNull(pid,ref));
    }
  }
}
