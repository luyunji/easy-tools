package org.luyunji.tools.snowflake;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class Snowflake {

    private Long startTimestamp;
    private final IpRange ipRange = new IpRange();
    private String businessType = "default";
    private Boolean useKeepBit = true;
    private Integer timestampBit = 41;
    private Integer appBit = 10;
    private Integer millisecondBit = 12;
    private String appSnowflakeTemp = "/tmp/snowflake.txt";

    private Path appSnowflakeTempPath;

    private final String space = " ";

    private final AtomicLong lastTimestamp = new AtomicLong(System.currentTimeMillis());

    private final AtomicLong sequence = new AtomicLong(0);

    private long sequenceMask;

    private long appSeq;

    private int timestampShift;
    private int appShift;


    /**
     * 获取分布式id
     *
     * @return id
     */
    //todo 后续优化成CAS
    public synchronized long nextId() {
        long currentTimestamp = System.currentTimeMillis();
        /*时钟回拨
        手动调整系统时间： 如果是手动调整系统时间造成的时钟回拨，一般来说会比较小，可能在几毫秒到几十毫秒的范围内。
        操作系统同步时间： 操作系统在与时间服务器同步时间时，可能会发生时钟回拨。这种情况下，回拨的时间跨度可能在几毫秒到一秒之间。
        虚拟化环境中的时钟回拨： 在虚拟化环境中，虚拟机可能会被暂停或者迁移导致时钟回拨。这种情况下，回拨的时间跨度可能在几毫秒到几秒之间。
        硬件故障： 如果是硬件故障导致的时钟回拨，可能会有较大的时间跨度，可能达到几十秒甚至更长的时间*/
        if (currentTimestamp < lastTimestamp.get()) {
            currentTimestamp = tilNextMillis(lastTimestamp.get());
        }
        /*当前毫秒自增尾位*/
        if (currentTimestamp == lastTimestamp.get()) {
            /*超出当前毫秒最大生成个数*/
            if ((sequence.incrementAndGet() & sequenceMask) == 0) {
                currentTimestamp = tilNextMillis(lastTimestamp.get());
            }
        } else {/*首个大于的时间的序号*/
            sequence.set(0L);
        }
        lastTimestamp.set(currentTimestamp);
        return ((currentTimestamp - startTimestamp) << timestampShift) |
                (appSeq << appShift) |
                sequence.get();
    }

    /**
     * 自旋等待获取下一毫秒，并设置尾位为0
     *
     * @return 下一毫秒
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        sequence.set(0L);
        return timestamp;
    }

    /**
     * 实例化
     *
     * @param configPath 自定义配置路径
     * @throws IOException 检查异常
     */
    public Snowflake(String configPath) throws IOException {
        loadCustomerProperties(configPath);
        checkParam();
    }

    public Snowflake(Long startTimestamp, InetAddress startIp, InetAddress endIp, String businessType, Boolean useKeepBit,
                     Integer timestampBit, Integer appBit, Integer millisecondBit, String appSnowflakeTemp) throws IOException {
        Optional.ofNullable(startIp).ifPresentOrElse(ipRange::setStart, () -> {
            throw new SnowflakeException("startIp can not blank");
        });
        Optional.ofNullable(endIp).ifPresentOrElse(ipRange::setEnd, () -> {
            throw new SnowflakeException("endIp can not blank");
        });
        Optional.ofNullable(startTimestamp).ifPresentOrElse(o -> this.startTimestamp = o, () -> {
            throw new SnowflakeException("startTimestamp can not blank");
        });
        Optional.ofNullable(businessType).ifPresent(o -> this.businessType = o);
        Optional.ofNullable(useKeepBit).ifPresent(o -> this.useKeepBit = o);
        Optional.ofNullable(timestampBit).ifPresent(o -> this.timestampBit = o);
        Optional.ofNullable(appBit).ifPresent(o -> this.appBit = o);
        Optional.ofNullable(millisecondBit).ifPresent(o -> this.millisecondBit = o);
        Optional.ofNullable(appSnowflakeTemp).ifPresent(o -> {
            this.appSnowflakeTemp = o;
            appSnowflakeTempPath = Paths.get(appSnowflakeTemp);
        });
        checkIpRanges();
        checkParam();
    }

    public Snowflake(Long startTimestamp, Long appId, String businessType, Boolean useKeepBit,
                     Integer timestampBit, Integer appBit, Integer millisecondBit, String appSnowflakeTemp) throws IOException {
        Optional.ofNullable(appId).ifPresentOrElse(o -> this.appSeq = o, () -> {
            throw new SnowflakeException("appId can not blank");
        });
        Optional.ofNullable(startTimestamp).ifPresentOrElse(o -> this.startTimestamp = o, () -> {
            throw new SnowflakeException("startTimestamp can not blank");
        });
        Optional.ofNullable(businessType).ifPresent(o -> this.businessType = o);
        Optional.ofNullable(useKeepBit).ifPresent(o -> this.useKeepBit = o);
        Optional.ofNullable(timestampBit).ifPresent(o -> this.timestampBit = o);
        Optional.ofNullable(appBit).ifPresent(o -> this.appBit = o);
        Optional.ofNullable(millisecondBit).ifPresent(o -> this.millisecondBit = o);
        Optional.ofNullable(appSnowflakeTemp).ifPresent(o -> {
            this.appSnowflakeTemp = o;
            appSnowflakeTempPath = Paths.get(appSnowflakeTemp);
        });
        if (appSeq <= 0) {
            throw new SnowflakeException("appId is must over 0");
        }
        checkParam();
    }

    /**
     * 检查配置项
     */
    private void checkParam() throws IOException {
        checkStartTimestamp();
        checkBusinessType();
        checkBit();
    }

    private void checkBit() {
        if (useKeepBit && timestampBit + appBit + millisecondBit != 63) {
            throw new SnowflakeException("bit sum must 63");
        } else if (!useKeepBit && timestampBit + appBit + millisecondBit != 64) {
            throw new SnowflakeException("bit sum must 64");
        }
        if ((appSeq >> appBit) > 0) {
            throw new SnowflakeException("appSeq is over bit");
        }
        sequenceMask = ~(-1L << millisecondBit);
        timestampShift = appBit + millisecondBit;
        appShift = millisecondBit;
    }

    /**
     * 检查业务类型是否在一台应用上重复
     *
     * @throws IOException 检查异常
     */
    private void checkBusinessType() throws IOException {
        if (Files.exists(appSnowflakeTempPath)) {
            List<String> fileResult = Files.readAllLines(appSnowflakeTempPath);
            Stream<String[]> contentStream = fileResult.stream().map(o -> o.split(space));
            List<String> allJavaProcess = getAllJavaProcess();
            Optional<String[]> first = contentStream.filter(o -> businessType.equals(o[0])).filter(o -> allJavaProcess.contains(o[1])).findFirst();
            if (first.isPresent()) {
                String[] businessTypeAndProcessId = first.get();
                throw new SnowflakeException(businessTypeAndProcessId[0] + " is used by process " + businessTypeAndProcessId[1]);
            } else {
                try (BufferedWriter writer = Files.newBufferedWriter(appSnowflakeTempPath, StandardOpenOption.APPEND)) {
                    writer.newLine();
                    writer.write(businessType + space + getProcessId());
                }
            }
        } else {
            Files.createFile(appSnowflakeTempPath);
            Files.writeString(appSnowflakeTempPath, businessType + space + getProcessId());
        }
    }

    private String getProcessId() {
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        return runtimeName.split("@")[0];
    }

    private List<String> getAllJavaProcess() throws IOException {
        Process process = Runtime.getRuntime().exec("jps");
        String line;
        List<String> result = new ArrayList<>();
        try (InputStreamReader inputStreamReader = new InputStreamReader(process.getInputStream());
             BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
            while ((line = bufferedReader.readLine()) != null) {
                result.add(line.split(space)[0]);
            }
        }
        return result;
    }

    /**
     * 检查ip范围
     *
     * @throws UnknownHostException 检查异常
     */
    private void checkIpRanges() throws UnknownHostException {
        long startIp = byteArrayToLong(ipRange.getStart().getAddress());
        long endIp = byteArrayToLong(ipRange.getEnd().getAddress());
        if (endIp < startIp) {
            throw new SnowflakeException("ipRange must from small to large");
        }
        long offset = endIp - startIp + 1;
        if (offset >> appBit > 0) {
            throw new SnowflakeException("ips is over ipBit");
        }
        InetAddress localHost = InetAddress.getLocalHost();
        byte[] address = localHost.getAddress();
        long addressIp = byteArrayToLong(address);
        if (addressIp < startIp || addressIp > endIp) {
            throw new SnowflakeException("Local Address IP is low startIp or is large endIp");
        }
        appSeq = addressIp - startIp;
    }

    public long byteArrayToLong(byte[] bytes) {
        // 将字节数组中的每个字节与 0xFF 进行与运算，确保每个字节转换为正数
        long value = 0L;
        for (int i = 0; i < 4; i++) {
            value |= (long) (bytes[i] & 0xFF) << (8 * (3 - i));
        }
        return value;
    }

    /**
     * 检查时间戳的值大小
     */
    private void checkStartTimestamp() {
        long now = System.currentTimeMillis();
        long offset = now - startTimestamp;
        if (offset + 2L * 12 * 365 * 24 * 60 * 60 * 1000 >> timestampBit > 0) {
            System.out.println("the timestampBit close to exhaustion");
        }
        if (offset >> timestampBit > 0) {
            throw new SnowflakeException("timestamp is over timestampBit");
        }
    }

    /**
     * 加载默认配置到实例中
     */
    private void loadCustomerProperties(String configPath) throws IOException {
        InputStream resourceAsStream = Snowflake.class.getResourceAsStream(configPath);
        Properties properties = new Properties();
        properties.load(resourceAsStream);
        String startTimestampStr = properties.getProperty("snowflake.startTimestamp");
        if (Objects.isNull(startTimestampStr) || startTimestampStr.isBlank()) {
            throw new SnowflakeException("IpSnowflake.startTimestamp can not blank");
        }
        startTimestamp = Long.parseLong(startTimestampStr);
        String appId = properties.getProperty("snowflake.appId");
        if (Objects.isNull(appId) || appId.isBlank()) {
            String ipRangesStr = properties.getProperty("snowflake.ipRanges");
            if (Objects.isNull(ipRangesStr) || ipRangesStr.isBlank()) {
                throw new SnowflakeException("IpSnowflake.ipRanges can not blank");
            }
            String[] split = ipRangesStr.split("-");
            if (split.length != 2) {
                throw new SnowflakeException("IpSnowflake.ipRanges format must be 192.168.x.x-192.168.x.x");
            }
            ipRange.setStart(InetAddress.getByName(split[0]));
            ipRange.setEnd(InetAddress.getByName(split[1]));
        } else {
            appSeq = Long.parseLong(appId);
            if (appSeq <= 0) {
                throw new SnowflakeException("IpSnowflake.appId is must over 0");
            }
        }
        String businessTypeStr = properties.getProperty("snowflake.businessType");
        if (Objects.nonNull(businessTypeStr) && !businessTypeStr.isBlank()) {
            businessType = businessTypeStr;
        }
        String useKeepBitStr = properties.getProperty("snowflake.useKeepBit");
        if (Objects.nonNull(useKeepBitStr) && !useKeepBitStr.isBlank()) {
            if (useKeepBitStr.equalsIgnoreCase(Boolean.FALSE.toString())) {
                useKeepBit = Boolean.FALSE;
            } else if (useKeepBitStr.equalsIgnoreCase(Boolean.TRUE.toString())) {
                useKeepBit = Boolean.TRUE;
            } else {
                throw new SnowflakeException("IpSnowflake.useKeepBit only use boolean true or false");
            }
        }
        String timestampBitStr = properties.getProperty("snowflake.timestampBit");
        if (Objects.nonNull(timestampBitStr) && !timestampBitStr.isBlank()) {
            timestampBit = Integer.parseInt(timestampBitStr);
        }
        String ipBitStr = properties.getProperty("snowflake.ipBit");
        if (Objects.nonNull(ipBitStr) && !ipBitStr.isBlank()) {
            appBit = Integer.parseInt(ipBitStr);
        }
        String millisecondBitStr = properties.getProperty("snowflake.millisecondBit");
        if (Objects.nonNull(millisecondBitStr) && !millisecondBitStr.isBlank()) {
            millisecondBit = Integer.parseInt(millisecondBitStr);
        }
        String ipSnowflakeTempStr = properties.getProperty("IpSnowflake.ipSnowflakeTemp");
        if (Objects.nonNull(ipSnowflakeTempStr) && !ipSnowflakeTempStr.isBlank()) {
            appSnowflakeTemp = ipSnowflakeTempStr;
            appSnowflakeTempPath = Paths.get(appSnowflakeTemp);
        }
    }

    public static class IpRange {
        private InetAddress start;
        private InetAddress end;

        public InetAddress getStart() {
            return start;
        }

        public void setStart(InetAddress start) {
            this.start = start;
        }

        public InetAddress getEnd() {
            return end;
        }

        public void setEnd(InetAddress end) {
            this.end = end;
        }
    }
}
