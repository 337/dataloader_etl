package com.xingcloud.util.manager;

import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.forex.ForexException;
import com.xingcloud.forex.ForexGetter;
import com.xingcloud.forex.ForexInfo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Author: qiujiawei ,ivy
 * Date:   11-7-15
 */
public class CurrencyManager {
    public static final Log LOG = LogFactory.getLog(DateManager.class);

    private Map<String, String> rateMap = new HashMap<String, String>();

    private CurrencyManager() throws ForexException, XCacheException {
        init();
    }

    /**
     * read the exchange.properties and store the exchange rate
     */
    private void init() throws ForexException, XCacheException {
        rateMap.clear();
        Set<ForexInfo> rates = ForexGetter.getAll();
        for (ForexInfo forexInfo : rates)
            rateMap.put(forexInfo.getCurrency().toLowerCase(), forexInfo.getRate().toString());
    }

    /**
     * return the currency rate or throw an Exception
     *
     * @param currency the currency want to query
     * @return the exchange rate or the 1.0
     * @throws Exception if the currency not find
     */
    private double getRate(String currency) throws Exception {
        String value = rateMap.get(currency.toLowerCase());
        if (value == null) throw new Exception("currency not find" + currency);
        else {
            return Double.valueOf(value);
        }
    }

    private static CurrencyManager instance;

    private static CurrencyManager getInstance() throws ForexException, XCacheException {
        if (instance == null) instance = new CurrencyManager();
        return instance;
    }

    /**
     * return the currency rate or throw an Exception
     *
     * @param currency the currency want to query
     * @return the exchange rate or the 1.0
     * @throws Exception if the currency not find
     */
    public static double getCurrencyRate(String currency) throws Exception {
        return CurrencyManager.getInstance().getRate(currency);
    }

    /**
     * calculate the amount of value currency at USD
     * the result will turn into int
     *
     * @param value    the amount of currency
     * @param currency the currency
     * @return the amount at USD
     * @throws Exception if the currency not find
     */
    public static int calculateAmount(double value, String currency) throws Exception {
        double rate = CurrencyManager.getCurrencyRate(currency);
        return (int) (value * 1000 / 10 / rate);
    }

    public static int calculateAmount(String value, String currency) throws Exception {

        return CurrencyManager.calculateAmount(Double.valueOf(value), currency);
    }

    public static void cleanUp() {
        instance = null;
    }
}
