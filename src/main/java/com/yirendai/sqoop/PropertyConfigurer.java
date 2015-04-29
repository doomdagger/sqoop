/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  The ASF licenses this file to You
 * under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.  For additional information regarding
 * copyright in this work, please see the NOTICE file in the top level
 * directory of this distribution.
 */

package com.yirendai.sqoop;

import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * This is the single entry point for accessing configuration properties in Roller.
 */
public final class PropertyConfigurer {

    private static final String default_config = "/com/yirendai/sqoop/worker.properties";
    private static final String custom_config = "/client-custom.properties";

    private static Properties config;

    private static Log log = LogFactory.getLog(PropertyConfigurer.class);


    /*
     * Static block run once at class loading
     *
     * We load the default properties and any custom properties we find
     */
    static {
        config = new Properties();

        try {
            // we'll need this to get at our properties files in the classpath
            Class configClass = Class.forName("com.yirendai.sqoop.PropertyConfigurer");

            // first, lets load our default properties
            InputStream is = configClass.getResourceAsStream(default_config);
            config.load(is);

            // now, see if we can find our custom config
            is = configClass.getResourceAsStream(custom_config);
            if (is != null) {
                config.load(is);
                log.info("Avro RPC Client: Successfully loaded custom properties file from classpath");
                log.info("File path : " + configClass.getResource(custom_config).getFile());
            } else {
                log.info("Avro RPC Client: No custom properties file found in classpath");
            }


            // finally we can start logging...

            // some debugging for those that want it
            if (log.isDebugEnabled()) {
                log.debug("Avro RPC Client looks like this ...");

                String key = null;
                Enumeration keys = config.keys();
                while (keys.hasMoreElements()) {
                    key = (String) keys.nextElement();
                    log.debug(key + "=" + config.getProperty(key));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    // no, you may not instantiate this class :p
    private PropertyConfigurer() {
    }


    /**
     * Retrieve a property value
     *
     * @param key Name of the property
     * @return String Value of property requested, null if not found
     */
    public static String getProperty(String key) {
        log.debug("Fetching property [" + key + "=" + config.getProperty(key) + "]");
        String value = config.getProperty(key);
        return value == null ? null : value.trim();
    }

    /**
     * Retrieve a property value
     *
     * @param key          Name of the property
     * @param defaultValue Default value of property if not found
     * @return String Value of property requested or defaultValue
     */
    public static String getProperty(String key, String defaultValue) {
        log.debug("Fetching property [" + key + "=" + config.getProperty(key) + ",defaultValue=" + defaultValue + "]");
        String value = config.getProperty(key);
        if (value == null) {
            return defaultValue;
        }

        return value.trim();
    }

    /**
     * Retrieve a property as a boolean ... defaults to false if not present.
     */
    public static boolean getBooleanProperty(String name) {
        return getBooleanProperty(name, false);
    }

    /**
     * Retrieve a property as a boolean ... with specified default if not present.
     */
    public static boolean getBooleanProperty(String name, boolean defaultValue) {
        // get the value first, then convert
        String value = PropertyConfigurer.getProperty(name);

        if (value == null) {
            return defaultValue;
        }

        return Boolean.valueOf(value);
    }

    /**
     * Retrieve a property as an int ... defaults to 0 if not present.
     */
    public static int getIntProperty(String name) {
        return getIntProperty(name, 0);
    }

    /**
     * Retrieve a property as a int ... with specified default if not present.
     */
    public static int getIntProperty(String name, int defaultValue) {
        // get the value first, then convert
        String value = PropertyConfigurer.getProperty(name);

        if (value == null) {
            return defaultValue;
        }

        return Integer.valueOf(value);
    }

    /**
     * Retrieve all property keys
     *
     * @return Enumeration A list of all keys
     */
    public static Enumeration keys() {
        return config.keys();
    }


    /**
     * Get properties starting with a specified string.
     */
    public static Properties getPropertiesStartingWith(String startingWith) {
        Properties props = new Properties();
        for (Enumeration it = config.keys(); it.hasMoreElements(); ) {
            String key = (String) it.nextElement();
            props.put(key, config.get(key));
        }
        return props;
    }

    /**
     * Get All Properties
     * @return
     */
    public static Properties getProperties() {
        return config;
    }
}