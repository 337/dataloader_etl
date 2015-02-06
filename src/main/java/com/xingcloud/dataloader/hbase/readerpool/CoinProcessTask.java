package com.xingcloud.dataloader.hbase.readerpool;

import com.xingcloud.dataloader.hbase.table.user.CoinBuyCacheMap;
import com.xingcloud.dataloader.hbase.table.user.CoinInitialStateCacheMap;
import com.xingcloud.dataloader.hbase.table.user.CoinPromotionCacheMap;
import com.xingcloud.dataloader.hbase.tableput.TablePut;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.SeqUidCacheMapV2;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 5/31/13
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class CoinProcessTask /*implements Runnable*/{
    public static final Log LOG = LogFactory.getLog(ReaderTask.class);
    private static long tasknum=0;
    private long mytasknum=0;
    private String project;
    private TablePut tablePut;
    private List<Event> eventList;
    private Map<String,Connection> cons;
    //private Event event;
    private static String BUYTABLE="coin_buy";
    private static String PROMOTIONTABLE="coin_promotion";
    private static String INITIALTABLE="coin_initialstatus";
    public CoinProcessTask(String project,List<Event> eventList,TablePut tableput){
        this.project=project;
        this.tablePut=tableput;
        //this.event=event;
        tasknum++;
        mytasknum=tasknum;
        this.eventList=eventList;
        cons=new HashMap<String,Connection>();
    }
    public void run(){
        LOG.info("begin run coin task "+project+". Task num is "+mytasknum);
        long t1 = System.currentTimeMillis();
        int successfulNum=0;
        getCoinValues(eventList);
        long t2= System.currentTimeMillis();
        long mysqlTime=t2-t1;
        LOG.info(project+" get data from mysql using "+mysqlTime+" ms");
        for(Event event: eventList){
            List<Event> result=getEventsFromCoin(project,event);
            for(Event tmp: result){
                if(tablePut.put(tmp))successfulNum++;
            }
        }
        long t3=System.currentTimeMillis();
        long useTime=t3-t1;

        LOG.info(project+" process coinEvents using "+useTime+"ms");
        LOG.info(project+" successfully process coinEvents "+successfulNum);
        LOG.info(project+" end run coin task "+mytasknum);
    }

    private void getCoinValues(List<Event> eventList){
         CoinBuyCacheMap.getInstance().getCoinValues(project,eventList,cons);
         CoinInitialStateCacheMap.getInstance().getCoinValues(project,eventList,cons);
         CoinPromotionCacheMap.getInstance().getCoinValues(project,eventList,cons);
         for(Map.Entry<String,Connection> entry: cons.entrySet()){
            try {
                entry.getValue().close();
            } catch (SQLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
         }
    }
    private List<Event> getEventsFromCoin(String project, Event origEvent) {
        String eventStr=origEvent.getEvent().substring(0,origEvent.getEvent().length()-1);
        String json=null;
        String uid=origEvent.getUid();
        List<Event> result=new ArrayList<Event>();
        result.add(origEvent);
        long ts=origEvent.getTimestamp();
        Long value=origEvent.getValue();
        String [] t=eventStr.split("\\.");
        String event[]=new String[Event.eventFieldLength];
        for (int i = 0; i <t.length; i++) {
            event[i]=t[i];
        }
        if(eventStr.contains("audit.produce.buy.coin")||eventStr.contains("audit.produce.promotion.coin")){
            try {
                long seqUid= SeqUidCacheMapV2.getInstance().getUidCache(project,uid);
                long samplingSeqUid= UidMappingUtil.getInstance().decorateWithMD5(seqUid);
                int coin_buy_val=0,coin_promotion_val=0;
                if(event[2].equals("buy")){
                    coin_buy_val= CoinBuyCacheMap.getInstance().getVal(project,samplingSeqUid);
                    coin_buy_val+=value;
                    CoinBuyCacheMap.getInstance().put(project,samplingSeqUid,coin_buy_val);
                }else if(event[2].equals("promotion")){
                    coin_promotion_val= CoinPromotionCacheMap.getInstance().getVal(project,samplingSeqUid);
                    coin_promotion_val+=value;
                    CoinPromotionCacheMap.getInstance().put(project,samplingSeqUid,coin_promotion_val);
                }
                //result.add(new Event(uid,event,value,ts,json));
                String []updateEvents=new String[Event.eventFieldLength];
                updateEvents[0]="update";
                JSONObject jsonObject=new JSONObject();
                if(event[2].equals("buy")){
                    jsonObject.put("coin_buy",value);
                    //LOG.info(seqUid+" buy.coin,val:"+value);
                    //LOG.info(seqUid+" coin_buy,val:"+coin_buy_val);
                }
                else if(event[2].equals("promotion")){
                    jsonObject.put("coin_promotion",value);
                    //LOG.info(seqUid+" promotion.coin,val:"+value);
                    //LOG.info(seqUid+" coin_promotion,val:"+coin_promotion_val);
                }
                json=jsonObject.toString();
                result.add(new Event(uid,updateEvents,value,ts,json));
            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }else if(eventStr.contains("audit.consume.cost")){
            try {
                long seqUid=SeqUidCacheMapV2.getInstance().getUidCache(project,uid);
                long samplingSeqUid=UidMappingUtil.getInstance().decorateWithMD5(seqUid);
                int coin_initial_status= CoinInitialStateCacheMap.getInstance().getVal(project,samplingSeqUid);
                int coin_buy_val=CoinBuyCacheMap.getInstance().getVal(project,samplingSeqUid);
                int coin_promotion_val=CoinPromotionCacheMap.getInstance().getVal(project,samplingSeqUid);
                int cost_coin_buy_val=0,cost_coin_promotion_val=0;
                long change_coin_buy=0,change_coin_promotion=0;
                double ratio=0.5;
                if(coin_buy_val>0 && coin_promotion_val>0)
                    ratio=((double)coin_buy_val)/(double)(coin_buy_val+coin_promotion_val);
                else if(coin_buy_val<0 && coin_promotion_val<0)
                    ratio=1-((double)coin_buy_val)/(double)(coin_buy_val+coin_promotion_val);
                else if(coin_buy_val<=0 && coin_promotion_val>0)
                    ratio=0;
                else if(coin_buy_val>0 && coin_promotion_val<=0)
                    ratio=1;
                String[] updateEvent=new String[Event.eventFieldLength];
                updateEvent[0]="update";
                JSONObject jsonObject=new JSONObject();
                String[] consumeBuyCoinEvent=new String[Event.eventFieldLength];
                String[] consumePromotionCoinEvent=new String[Event.eventFieldLength];
                for (int i = 0; i < 2; i++) {
                    consumeBuyCoinEvent[i]=event[i];
                    consumePromotionCoinEvent[i]=event[i];
                }
                consumeBuyCoinEvent[2]="calcost";
                consumePromotionCoinEvent[2]="calcost";
                if(eventStr.contains("coin")){
                    consumeBuyCoinEvent[3]="coinbuy";
                    consumePromotionCoinEvent[3]="coinpromotion";
                }else if(eventStr.contains("item")){
                    consumeBuyCoinEvent[3]="itembuy";
                    if(t.length>=5)consumeBuyCoinEvent[4]=event[4];
                    consumePromotionCoinEvent[3]="itempromotion";
                    if(t.length>=5)consumePromotionCoinEvent[4]=event[4];
                }

                if(value<=coin_initial_status){
                    LOG.info("initial status "+coin_initial_status+" is bigger than cost "+value);
                    coin_initial_status-=value;
                    CoinInitialStateCacheMap.getInstance().put(project,samplingSeqUid,coin_initial_status);
                    jsonObject.put(INITIALTABLE,-value);
                    cost_coin_buy_val=(int)(value*ratio);
                    cost_coin_promotion_val=(int)(value-cost_coin_buy_val);
                }
                else {
                    if(coin_initial_status!=0){
                        LOG.info("initial status "+coin_initial_status+" is smaller than cost "+value);
                        cost_coin_buy_val=(int)(value*ratio);
                        cost_coin_promotion_val=(int)(value-cost_coin_buy_val);
                        change_coin_buy=(long)(cost_coin_buy_val-ratio*coin_initial_status);
                        change_coin_promotion=(long)(cost_coin_promotion_val-(1-ratio)*coin_initial_status);
                        CoinInitialStateCacheMap.getInstance().put(project,samplingSeqUid,0);
                        jsonObject.put(INITIALTABLE,-coin_initial_status);
                    }
                    else{
                        LOG.info("initial status is 0. while cost is "+value);
                        cost_coin_buy_val=(int)(value*ratio);
                        cost_coin_promotion_val=(int)(value-cost_coin_buy_val);
                        change_coin_buy=(long)cost_coin_buy_val;
                        change_coin_promotion=(long)cost_coin_promotion_val;
                    }
                    coin_buy_val-=change_coin_buy;
                    coin_promotion_val-=change_coin_promotion;
                    CoinBuyCacheMap.getInstance().put(project,samplingSeqUid,coin_buy_val);
                    CoinPromotionCacheMap.getInstance().put(project,samplingSeqUid,coin_promotion_val);
                    //cost_coin_promotion_val=(int)(value*(1-ratio));
                    //coin_buy_val-=cost_coin_buy_val;
                    //coin_promotion_val-=cost_coin_promotion_val;
                    jsonObject.put("coin_buy",-change_coin_buy);
                    jsonObject.put("coin_promotion",-change_coin_promotion);
                }
	    LOG.info(uid+" coinratio:"+ratio);
            LOG.info(uid+" calcost.coinbuy: "+cost_coin_buy_val);
            LOG.info(uid+" calcost.coinpromotion: "+cost_coin_promotion_val);
	    LOG.info(uid+" cost.coin: "+value);
            result.add(new Event(uid,consumeBuyCoinEvent,cost_coin_buy_val,ts,json));
            result.add(new Event(uid,consumePromotionCoinEvent,cost_coin_promotion_val,ts,json));
            json=jsonObject.toString();
            LOG.info(json);
            result.add(new Event(uid,updateEvent,value,ts,json));

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        }
        return result;
        //return null;  //To change body of created methods use File | Settings | File Templates.
    }

}
