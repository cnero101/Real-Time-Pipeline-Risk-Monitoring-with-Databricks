# Databricks notebook source

    %sql
    -- Widget 1: KPI summary — headline numbers for ops team
    SELECT
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_events,
        SUM(CASE WHEN severity = 'WARNING'  THEN 1 ELSE 0 END) AS warning_events,
        SUM(CASE WHEN severity = 'NORMAL'   THEN 1 ELSE 0 END) AS normal_events,
        COUNT(DISTINCT pipe_segment_id)                         AS segments_monitored,
        COUNT(DISTINCT CASE WHEN severity = 'CRITICAL' 
            THEN pipe_segment_id END)                           AS segments_critical,
        ROUND(AVG(CASE WHEN severity = 'CRITICAL' 
            THEN pressure_psi END), 1)                          AS avg_critical_pressure,
        ROUND(MIN(CASE WHEN severity = 'CRITICAL' 
            THEN pressure_psi END), 1)                          AS lowest_pressure_detected
    FROM main.pipeline_leak.gold_alerts
  
    %sql
    -- Widget 2: Segment risk summary
    SELECT
        pipe_segment_id,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_events,
        SUM(CASE WHEN severity = 'WARNING'  THEN 1 ELSE 0 END) AS warning_events,
        ROUND(MIN(pressure_psi), 1)                             AS min_pressure,
        ROUND(AVG(pressure_psi), 1)                             AS avg_pressure,
        ROUND(MAX(acoustic_g_rms), 4)                           AS max_acoustic
    FROM main.pipeline_leak.gold_alerts
    GROUP BY pipe_segment_id
    ORDER BY critical_events DESC
  
    %sql
    -- Widget 3: Latest reading per segment with full detail
    SELECT
        pipe_segment_id,
        severity,
        ROUND(pressure_psi, 1)          AS pressure_psi,
        ROUND(flow_rate_lpm, 1)         AS flow_lpm,
        ROUND(acoustic_g_rms, 3)        AS acoustic_grms,
        ROUND(temperature_c, 1)         AS temp_c,
        ROUND(anomaly_score, 4)         AS anomaly_score,
        alert_message,
        event_timestamp
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY pipe_segment_id
                ORDER BY event_timestamp DESC
            ) AS rn
        FROM main.pipeline_leak.gold_alerts
    ) WHERE rn = 1
    ORDER BY 
        CASE severity 
            WHEN 'CRITICAL' THEN 1 
            WHEN 'WARNING'  THEN 2 
            ELSE 3 
        END
   
    %sql
    -- Widget 4: Pressure health per segment
    -- Shows normal range, actual average, and minimum (leak level)
    SELECT
        pipe_segment_id,
        ROUND(AVG(pressure_psi), 1)                              AS avg_pressure,
        ROUND(MIN(pressure_psi), 1)                              AS min_pressure,
        ROUND(MAX(pressure_psi), 1)                              AS max_pressure,
        ROUND(AVG(CASE WHEN severity = 'CRITICAL' 
            THEN pressure_psi END), 1)                           AS avg_critical_pressure,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END)  AS critical_count,
        SUM(CASE WHEN severity = 'WARNING'  THEN 1 ELSE 0 END)  AS warning_count,
        ROUND(AVG(acoustic_g_rms), 4)                           AS avg_acoustic,
        ROUND(AVG(flow_rate_lpm), 1)                            AS avg_flow
    FROM main.pipeline_leak.gold_alerts
    GROUP BY pipe_segment_id
    ORDER BY critical_count DESC
   
    %sql
    -- Widget 5: Critical count + worst anomaly score per segment
    SELECT
        pipe_segment_id,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN severity = 'WARNING'  THEN 1 ELSE 0 END) AS warning_count,
        ROUND(MIN(anomaly_score), 4)                            AS worst_anomaly_score,
        ROUND(AVG(anomaly_score), 4)                            AS avg_anomaly_score,
        ROUND(SUM(anomaly_score), 4)                            AS cumulative_anomaly_impact
    FROM main.pipeline_leak.gold_alerts
    WHERE severity IN ('CRITICAL', 'WARNING')
    GROUP BY pipe_segment_id
    ORDER BY critical_count DESC
   
    %sql
    -- Widget 6: Acoustic vibration health per segment
    SELECT
        pipe_segment_id,
        ROUND(AVG(acoustic_g_rms), 4)                           AS avg_acoustic,
        ROUND(MAX(acoustic_g_rms), 4)                           AS max_acoustic,
        ROUND(AVG(CASE WHEN severity = 'NORMAL' 
            THEN acoustic_g_rms END), 4)                        AS normal_acoustic,
        ROUND(AVG(CASE WHEN severity = 'CRITICAL' 
            THEN acoustic_g_rms END), 4)                        AS critical_acoustic,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count
    FROM main.pipeline_leak.gold_alerts
    GROUP BY pipe_segment_id
    ORDER BY max_acoustic DESC
   
    %sql
    -- Widget 7: Flow rate imbalance per segment
    SELECT
        pipe_segment_id,
        ROUND(AVG(flow_rate_lpm), 1)                            AS avg_flow,
        ROUND(MAX(flow_rate_lpm), 1)                            AS max_flow,
        ROUND(AVG(CASE WHEN severity = 'NORMAL' 
            THEN flow_rate_lpm END), 1)                         AS normal_flow,
        ROUND(AVG(CASE WHEN severity = 'CRITICAL' 
            THEN flow_rate_lpm END), 1)                         AS critical_flow,
        ROUND(AVG(flow_imbalance_ratio), 3)                     AS avg_imbalance_ratio,
        ROUND(MAX(flow_imbalance_ratio), 3)                     AS max_imbalance_ratio
    FROM main.pipeline_leak.gold_alerts
    GROUP BY pipe_segment_id
    ORDER BY max_imbalance_ratio DESC
   
    %sql
    -- Widget 8: Complete CRITICAL alert audit log
    SELECT
        event_timestamp,
        pipe_segment_id,
        severity,
        ROUND(pressure_psi, 1)          AS pressure_psi,
        ROUND(pressure_delta, 1)        AS pressure_delta,
        ROUND(flow_rate_lpm, 1)         AS flow_lpm,
        ROUND(acoustic_g_rms, 3)        AS acoustic_grms,
        ROUND(anomaly_score, 4)         AS anomaly_score,
        alert_message
    FROM main.pipeline_leak.gold_alerts
    WHERE severity = 'CRITICAL'
    ORDER BY anomaly_score ASC, event_timestamp DESC
   
    %sql
    -- Widget 9: Temperature comparison per segment
    SELECT
      pipe_segment_id,
      ROUND(AVG(temperature_c), 1) AS avg_temp,
      ROUND(MAX(temperature_c), 1) AS max_temp,
      ROUND(
        AVG(
          CASE
            WHEN severity = 'NORMAL' THEN temperature_c
          END
        ),
        1
      ) AS normal_temp,
      ROUND(
        AVG(
          CASE
            WHEN severity = 'CRITICAL' THEN temperature_c
          END
        ),
        1
      ) AS critical_temp,
      ROUND(
        AVG(
          CASE
            WHEN severity = 'CRITICAL' THEN temperature_c
          END
        )
          - AVG(
            CASE
              WHEN severity = 'NORMAL' THEN temperature_c
            END
          ),
        1
      ) AS temp_delta,
      SUM(
        CASE
          WHEN severity = 'CRITICAL' THEN 1
          ELSE 0
        END
      ) AS critical_count
    FROM
      main.pipeline_leak.gold_alerts
    GROUP BY
      pipe_segment_id
    ORDER BY
      ABS(temp_delta) DESC
   
    %sql
    -- Widget 10: Severity distribution for pie chart
    SELECT
      'CRITICAL' AS severity_level,
      COUNT(*) AS event_count
    FROM
      main.pipeline_leak.gold_alerts
    WHERE
      severity = 'CRITICAL'
    UNION ALL
    SELECT
      'WARNING' AS severity_level,
      COUNT(*) AS event_count
    FROM
      main.pipeline_leak.gold_alerts
    WHERE
      severity = 'WARNING'
    UNION ALL
    SELECT
      'NORMAL' AS severity_level,
      COUNT(*) AS event_count
    FROM
      main.pipeline_leak.gold_alerts
    WHERE
      severity = 'NORMAL'
   
    %sql
    -- Widget 11: Waterfall: Cumulative anomaly impact per segment
    SELECT
        pipe_segment_id,
        ROUND(AVG(anomaly_score), 4)                            AS avg_anomaly_score,
        ROUND(MIN(anomaly_score), 4)                            AS worst_anomaly_score,
        ROUND(SUM(anomaly_score), 4)                            AS cumulative_anomaly_impact,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) AS critical_count,
        SUM(CASE WHEN severity = 'WARNING'  THEN 1 ELSE 0 END) AS warning_count,
        COUNT(*)                                                AS total_anomalies
    FROM main.pipeline_leak.gold_alerts
    WHERE severity IN ('CRITICAL', 'WARNING')
    GROUP BY pipe_segment_id
    ORDER BY cumulative_anomaly_impact ASC
   
