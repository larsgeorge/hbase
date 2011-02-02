package org.apache.hadoop.hbase.thrift2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class HBaseAdminPool {
  private final Queue<HBaseAdmin> admins = new ConcurrentLinkedQueue<HBaseAdmin>();
  private final Configuration config;
  private final int maxSize;

  /**
   * Default Constructor.  Default HBaseConfiguration and no limit on pool size.
   */
  public HBaseAdminPool() {
    this(HBaseConfiguration.create(), Integer.MAX_VALUE);
  }

  /**
   * Constructor to set maximum versions and use the specified configuration.
   * @param config configuration
   * @param maxSize maximum number of references to keep for each table
   */
  public HBaseAdminPool(Configuration config, int maxSize) {
    this.config = config;
    this.maxSize = maxSize;
  }

  /**
   * Get a reference to a HBaseAdmin.
   * Create a new one if one is not available.
   *
   * @return a HBaseAdmin
   * @throws RuntimeException if there is a problem instantiating the HTable
   */
  public HBaseAdmin getAdmin() throws MasterNotRunningException {
    HBaseAdmin admin = admins.poll();
    if (admin == null) {
      return createHBaseAdmin();
    }
    return admin;
  }

  /**
   * Puts the specified HBaseAdmin back into the pool.<p>
   *
   * If the pool already contains <i>maxSize</i> references to the table,
   * then nothing happens.
   * @param admin
   */
  public void putAdmin(HBaseAdmin admin) {
    if (admins.size() >= maxSize) return;
    admins.add(admin);
  }

  protected HBaseAdmin createHBaseAdmin() throws MasterNotRunningException {
    return new HBaseAdmin(config);
  }

}
