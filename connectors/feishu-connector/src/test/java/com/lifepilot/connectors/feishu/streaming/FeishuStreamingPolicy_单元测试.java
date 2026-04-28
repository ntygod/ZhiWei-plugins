package com.lifepilot.connectors.feishu.streaming;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * FeishuStreamingPolicy 节流策略单元测试。
 *
 * @author zsg
 * @since 2026-04-28
 */
class FeishuStreamingPolicy_单元测试 {

    @Test
    void 前5秒内_最小间隔200ms() {
        var policy = FeishuStreamingPolicy.defaults();
        Instant t0 = Instant.parse("2026-04-28T00:00:00Z");
        // elapsed=2s（首字节窗口内）→ 200ms
        assertThat(policy.minInterval(t0, t0.plusMillis(2000))).isEqualTo(Duration.ofMillis(200));
        // elapsed=10s（已过 5s 期）→ 1500ms
        assertThat(policy.minInterval(t0, t0.plusSeconds(10))).isEqualTo(Duration.ofMillis(1500));
    }

    @Test
    void 退避时长指数_1_2_4秒() {
        var policy = FeishuStreamingPolicy.defaults();
        assertThat(policy.backoffDuration(0)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.backoffDuration(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.backoffDuration(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.backoffDuration(3)).isEqualTo(Duration.ofSeconds(4));  // cap
    }

    @Test
    void idle_超过30s_视为_rid_结束() {
        var policy = FeishuStreamingPolicy.defaults();
        Instant lastEnqueue = Instant.parse("2026-04-28T00:00:00Z");
        assertThat(policy.isIdle(lastEnqueue, lastEnqueue.plusSeconds(31))).isTrue();
        assertThat(policy.isIdle(lastEnqueue, lastEnqueue.plusSeconds(20))).isFalse();
    }
}
