/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeffy.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

/**
 * @Author Jeffy
 * @Email: renwu58@gmail.com
 * 
 *         用于测试Phoenix的JDBC插入速度
 * 
 */
public class PhoenixTest {

	private String url = "jdbc:phoenix://localhost:2181";
	// 用于产生随机字符串
	public static final char[] subset = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();
	private static final ThreadLocalRandom randon = ThreadLocalRandom.current();

	private String user = "test";

	private String pass = "test";

	private int numberOfThreads;

	private int numberOfRows = 100000;

	private final ExecutorService executor;

	private long beginTs;

	/**
	 * 参数： JDBC 连接信息： jdbc:phoenix [ :<zookeeper quorum> [ :<port number> ] [
	 * :<root node> ] [ :<principal> ] [ :<keytab file> ] ] 每个表插入的数据行数 [默认10W]
	 * 模拟的线程数[默认与CPU核心数一致]
	 * 
	 */
	public static void main(String[] args) {
		PhoenixTest test = null;
		int len = args.length;
		if (len > 0) {
			test = new PhoenixTest(args[0]);
		} else {
			showHelp();
			System.exit(1);
		}
		if (len > 1) {
			test = new PhoenixTest(args[0], Integer.valueOf(args[1]));
		}
		if (len > 2) {
			test = new PhoenixTest(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]));
		}
		if (test == null) {
			showHelp();
		}
		test.start();
	}

	private static void showHelp() {
		System.out.println("==Test the Phoenix JDBC write performance==");
		System.out.println("Usage:");
		System.out.println("\t the first parameter is specify the Phoenix JDBC connection string.");
		System.out.println("\t the second parameter is number of rows each table, optional.");
		System.out.println(
				"\t the third parameter is number of thread used. each thread will response to a table, optional.");
	}

	public PhoenixTest() {
		numberOfThreads = Runtime.getRuntime().availableProcessors();
		numberOfRows = 100000;
		executor = Executors.newFixedThreadPool(numberOfThreads);
	}

	public PhoenixTest(String url) {
		this.url = url;
		numberOfThreads = Runtime.getRuntime().availableProcessors();
		numberOfRows = 100000;
		executor = Executors.newFixedThreadPool(numberOfThreads);
	}

	public PhoenixTest(String url, Integer numberOfRows) {
		this.url = url;
		this.numberOfRows = numberOfRows;
		numberOfThreads = Runtime.getRuntime().availableProcessors();
		executor = Executors.newFixedThreadPool(numberOfThreads);
	}

	public PhoenixTest(String url, Integer numberOfRows, Integer numberOfThreads) {
		this.url = url;
		this.numberOfRows = numberOfRows;
		this.numberOfThreads = numberOfThreads;
		executor = Executors.newFixedThreadPool(numberOfThreads);
	}

	/**
	 * 生成随机字符串
	 * 
	 * @param int
	 *            len
	 * @return
	 */
	public static String generateString(final int len) {
		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			int index = randon.nextInt(subset.length);
			chars[i] = subset[index];
		}
		return new String(chars);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(String pass) {
		this.pass = pass;
	}

	public int getNumberOfThreads() {
		return numberOfThreads;
	}

	public void setNumberOfThreads(int numberOfThreads) {
		this.numberOfThreads = numberOfThreads;
	}

	public int getNumberOfRows() {
		return numberOfRows;
	}

	public void setNumberOfRows(int numberOfRows) {
		this.numberOfRows = numberOfRows;
	}

	/**
	 * 开启测试任务
	 */
	public void start() {
		printHeader();
		ProduceTestData produce = new ProduceTestData(numberOfThreads, numberOfRows);
		ConsumeData consumer = new ConsumeData(produce, numberOfThreads, url);
		try {
			consumer.prepare();
		} catch (SQLException e) {
			e.printStackTrace();
			return;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		List<Future<?>> futures = new ArrayList<>();
		beginTs = System.currentTimeMillis();
		for (int i = 0; i < numberOfThreads / 2; i++) {
			futures.add(executor.submit(produce.createProducer(i)));
			futures.add(executor.submit(consumer.createConsumer(i)));
		}
		awaitCommpletion(futures);

	}

	/**
	 * 等待任务完成
	 * 
	 * @param futures
	 */
	private void awaitCommpletion(List<Future<?>> futures) {
		futures.forEach((future) -> {
			try {
				future.get();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		System.out
				.println("All task finished, total time: " + (System.currentTimeMillis() - beginTs) + " millseconds.");
		executor.shutdown();
	}

	private void printHeader() {
		System.out.println("================Test parameter used====================");
		System.out.println("Test url: " + url);
		System.out.println("Number of threads: " + numberOfThreads);
		System.out.println("Number of tables: " + numberOfThreads / 2);
		System.out.println("Rows in each table: " + numberOfRows);
		System.out.println("Total rows: " + numberOfRows * numberOfThreads / 2);
		System.out.println("=======================================================");
	}
}

/**
 * 一个生成数据的类
 */

class ProduceTestData {
	// 保存队列数据
	private List<ArrayBlockingQueue<Data>> dataPiplelines;
	// 每个线程使用的计数器
	private Map<Integer, AtomicLong> seqnoMap;
	// 每一个队列存储的数据量
	int size = 100;
	// 队列的个数
	int numberOfThreads;
	int numberOfRows;

	public ProduceTestData(int numberOfThreads, int numberOfRows) {
		this.numberOfThreads = numberOfThreads / 2;
		this.numberOfRows = numberOfRows;
		init();
	}

	public ProduceTestData(int numberOfThreads, int numberOfRows, int queueSize) {
		this.numberOfThreads = numberOfThreads / 2;
		this.numberOfRows = numberOfRows;
		this.size = queueSize;
		init();
	}

	/**
	 * 初始化准备
	 */
	private void init() {
		dataPiplelines = new ArrayList<ArrayBlockingQueue<Data>>(numberOfThreads);
		seqnoMap = new ConcurrentHashMap<Integer, AtomicLong>(numberOfThreads);
		for (int i = 0; i < numberOfThreads; i++) {
			dataPiplelines.add(new ArrayBlockingQueue<Data>(size));
			seqnoMap.put(i, new AtomicLong(0));
		}
	}

	/**
	 * 根据线程id获取对应的数据队列
	 * 
	 * @param threadId
	 * @return
	 */
	public ArrayBlockingQueue<Data> getDataQueue(int threadId) {
		if (threadId > numberOfThreads || threadId < 0) {
			throw new IndexOutOfBoundsException("Thread ID: " + threadId + " is not exists!");
		}
		return dataPiplelines.get(threadId);
	}

	public Runnable createProducer(final int id) {
		return () -> {
			// 获取到对应线程的队列
			ArrayBlockingQueue<Data> queue = dataPiplelines.get(id);
			// 获取对应线程的主键序列号
			AtomicLong seqnoBuilder = seqnoMap.get(id);
			long seqno = 0L;
			while (seqno < numberOfRows) {
				try {
					queue.put(buildData(seqno));
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}
				seqno = seqnoBuilder.incrementAndGet();
			}
			// 通知消费者数据已经产生完了
			try {
				queue.put(new ControlData());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		};
	}

	private Data buildData(long seqno) {
		TestData data = new TestData();
		data.setId(seqno);
		data.setName(PhoenixTest.generateString(15));
		data.setSessionId(System.currentTimeMillis());
		data.setCreateDate(new Date());
		data.setUpdateDate(new Date());
		data.setDescription(PhoenixTest.generateString(100));
		return data;
	}
}

/**
 * 一个消费数据的类
 */

class ConsumeData {
	private final static Logger logger = Logger.getLogger(ConsumeData.class);
	// 保存队列数据
	private ProduceTestData produce;
	// 保存数据库连接的Map
	private Map<Integer, Connection> linkMap;
	private String tablePrefix = "jeffy_";
	private String url;
	private int batchSize = 5000;

	private int numberOfThreads;
	public final static String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";
	public final static String createTable = "create table if not exists #tb# (tenantid varchar not null,id bigint not null,name varchar, createts timestamp,  updatets timestamp, sessionid bigint, description varchar, constraint pk primary key (tenantid,id, name) )SALT_BUCKETS=3,MULTI_TENANT=true";

	public final static String upsertStatement = "upsert into #tb# (id ,name , createts ,  updatets , sessionid , description) values (?,?,?,?,?,?)";

	public ConsumeData(ProduceTestData produce, int numberOfThreads, String url) {
		this.produce = produce;
		this.numberOfThreads = numberOfThreads / 2;
		this.url = url;
	}

	/**
	 * 准备数据库表
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	public void prepare() throws SQLException, ClassNotFoundException {
		Class.forName(PHOENIX_DRIVER);
		linkMap = new ConcurrentHashMap<>();
		for (int i = 0; i < numberOfThreads; i++) {
			Properties pops = new Properties();
			pops.put("TenantId", "test" + i);
			Connection conn = DriverManager.getConnection(url, pops);
			linkMap.put(i, conn);
			createTestTable(i);
		}
	}

	/**
	 * 创建测试的表
	 * 
	 * @param threadId
	 * @param conn
	 * @throws SQLException
	 */
	private void createTestTable(int threadId) throws SQLException {
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		String sql = createTable.replace("#tb#", tablePrefix + threadId);
		logger.info("===>" + sql);
		stmt.executeUpdate(sql);
		conn.commit();
		stmt.close();
		conn.close();
	}

	public Runnable createConsumer(final int id) {
		return () -> {
			ArrayBlockingQueue<Data> dataQueue = produce.getDataQueue(id);
			Connection conn = linkMap.get(id);
			PreparedStatement stmt = null;
			try {
				conn.setAutoCommit(false);
			} catch (SQLException e) {// 如果数据库不支持事物，直接忽略错误
				e.printStackTrace();
			}
			try {
				stmt = conn.prepareStatement(upsertStatement.replace("#tb#", tablePrefix + id));
			} catch (SQLException e1) {
				e1.printStackTrace(); // 如果无法创建PrepareStatement则 输出错误退出
				return;
			}
			int i = 0;
			while (true) {
				Data data;
				try {
					data = dataQueue.take();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
					commit(conn);
					break;
				}
				i++;
				// logger.info("========>"+data);
				if (data instanceof TestData) {
					try {
						executeUpsertStatement(stmt, (TestData) data);
						if (i % batchSize == 0) {
							conn.commit();
							System.out.print("+");
							if (i % (batchSize * 10) == 0) {
								System.out.println(id + "=" + dataQueue.size());
							}
						}
					} catch (SQLException e) {
						e.printStackTrace();
						break;
					}
				} else if (data instanceof ControlData) {
					break;
				} else {
					continue;
				}
			}
			commit(conn);
		};
	}

	private boolean commit(Connection conn) {
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	private void executeUpsertStatement(PreparedStatement stmt, TestData data) throws SQLException {
		TestData testData = (TestData) data;
		stmt.setLong(1, testData.getId());
		stmt.setString(2, testData.getName());
		stmt.setTimestamp(3, new Timestamp(testData.getCreateDate().getTime()));
		stmt.setTimestamp(4, new Timestamp(testData.getUpdateDate().getTime()));
		stmt.setLong(5, testData.getSessionId());
		stmt.setString(6, testData.getDescription());
		stmt.executeUpdate();
	}

}

interface Data {

}

class ControlData implements Data {

}

class TestData implements Data {
	private long id;
	private String name;
	private Date createDate;
	private Date updateDate;
	private long sessionId;
	private String description;

	/**
	 * 计算一个Data对象在文件中占用的字节数
	 * 
	 * @return
	 */
	public int size() {
		return 4 + name.length() + 8 + 8 + 8 + description.length();
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getCreateDate() {
		return createDate;
	}

	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}

	public Date getUpdateDate() {
		return updateDate;
	}

	public void setUpdateDate(Date updateDate) {
		this.updateDate = updateDate;
	}

	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		return "Data [id=" + id + ", name=" + name + ", createDate=" + createDate + ", updateDate=" + updateDate
				+ ", sessionId=" + sessionId + ", description=" + description + "]";
	}
}