/**
 * Copyright 2010 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * A filter that finds the currently active master and redirects to it from
 * a non-active master.
 */
public class MasterLocatorFilter implements Filter {

  private FilterConfig filterConfig = null;
  private InetAddress address = null;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    this.filterConfig = filterConfig;
    try {
      this.address = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new ServletException("Could not get own host name.", e);
    }
  }

  @Override
  public void destroy() {
    this.filterConfig = null;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    HMaster master = (HMaster) filterConfig.getServletContext().
      getAttribute(HMaster.MASTER);
    Configuration conf = master.getConfiguration();
    HBaseAdmin hbadmin = new HBaseAdmin(conf);
    HConnection connection = hbadmin.getConnection();
    ZooKeeperWrapper wrapper = connection.getZooKeeperWrapper();
    HServerAddress hsa = wrapper.readMasterAddressOrThrow();
    String ipAddr = hsa.getHostname();
    // compare local IP with current master IP address
    if (ipAddr != null && ipAddr.equals(address.getHostAddress())) {
      HttpServletRequest hreq = ((HttpServletRequest) request);
      HttpServletResponse hres = ((HttpServletResponse) response);
      // reconstruct URL with new address but same port and params
      StringBuffer url = new StringBuffer();
      url.append(hreq.getScheme());
      url.append("://");
      url.append(ipAddr);
      url.append(':');
      url.append(hreq.getServerPort());
      url.append(hreq.getRequestURI());
      String qry = hreq.getQueryString();
      if (qry != null) {
        url.append('?');
        url.append(qry);
      }
      // redirect to active master
      hres.sendRedirect(url.toString());
    } else {
      // or move on locally, i.e. let it pass
      chain.doFilter(request, response);
    }
  }

}
