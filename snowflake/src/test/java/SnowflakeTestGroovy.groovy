import org.luyunji.tools.snowflake.Snowflake
import org.luyunji.tools.snowflake.SnowflakeException
import org.mockito.Mockito
import spock.lang.Specification
import spock.lang.Unroll


class SnowflakeTestGroovy extends Specification {

    @Unroll
    def "test init exception,throw #message"() {
        given: "输入条件"
        def name = InetAddress.getByName("192.168.0.99")
        def mockStatic = Mockito.mockStatic(InetAddress.class, Mockito.CALLS_REAL_METHODS)
        mockStatic.when { InetAddress::getLocalHost() }.thenReturn(name)

        when: "入参配置"
        def snowflake = new Snowflake(startTimestamp, InetAddress.getByName(startIp), InetAddress.getByName(endIp), businessType, useKeepBit,
                timestampBit, ipBit, millisecondBit, ipSnowflakeTemp)

        then: "配置预期方法"
        def exception = thrown(ex)
        exception.message == message

        where: "验证异常抛出"
        startTimestamp | startIp       | endIp           | businessType | useKeepBit | timestampBit | ipBit | millisecondBit | ipSnowflakeTemp        | ex                   | message
        1710398951000  | "192.168.0.1" | "192.168.0.100" | "test"       | true       | 41           | 10    | 12             | "/tmp/IpSnowflake.txt" | SnowflakeException | "Local Address IP is low startIp or is large endIp"

    }

}
