package com.lifepilot.connectors.feishu.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FeishuConnectorService.inferInboundMimeType 单测。
 *
 * <p>飞书 msgType=file 的普通文件下载接口不返回 MIME，原实现硬编码 octet-stream
 * 导致主服务无法识别文档类型。这里按 fileName 扩展名兜底推断，修根因。</p>
 *
 * @author zsg
 * @since 2026-04-22
 */
class FeishuConnectorServiceInferMimeTest {

    @Test
    @DisplayName("xlsx 文件按扩展名推断为 spreadsheetml.sheet")
    void xlsx推断() {
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "report.xlsx"))
                .isEqualTo("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    }

    @Test
    @DisplayName("docx / pptx / pdf / md / csv 扩展名都能推断出真实 MIME")
    void 常见文档扩展名推断() {
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.docx"))
                .isEqualTo("application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.pptx"))
                .isEqualTo("application/vnd.openxmlformats-officedocument.presentationml.presentation");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.pdf"))
                .isEqualTo("application/pdf");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.md"))
                .isEqualTo("text/markdown");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.csv"))
                .isEqualTo("text/csv");
    }

    @Test
    @DisplayName("msgType=audio / media 保留原硬编码 MIME，fileName 仅作兜底")
    void audio_media类型优先() {
        assertThat(FeishuConnectorService.inferInboundMimeType("audio", "voice.ogg"))
                .isEqualTo("audio/ogg");
        assertThat(FeishuConnectorService.inferInboundMimeType("media", "clip.mp4"))
                .isEqualTo("video/mp4");
        // 即使 fileName 扩展名冲突也以 msgType 为准
        assertThat(FeishuConnectorService.inferInboundMimeType("audio", "voice.mp3"))
                .isEqualTo("audio/ogg");
    }

    @Test
    @DisplayName("未知扩展名 / 无扩展名 / fileName=null 都兜底 octet-stream")
    void 未知情况兜底() {
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "a.xyz"))
                .isEqualTo("application/octet-stream");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "noext"))
                .isEqualTo("application/octet-stream");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", null))
                .isEqualTo("application/octet-stream");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", ""))
                .isEqualTo("application/octet-stream");
    }

    @Test
    @DisplayName("大小写不敏感")
    void 大小写不敏感() {
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "REPORT.XLSX"))
                .isEqualTo("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        assertThat(FeishuConnectorService.inferInboundMimeType("file", "A.PdF"))
                .isEqualTo("application/pdf");
    }
}
