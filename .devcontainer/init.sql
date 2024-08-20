-- Create sleep_data table
CREATE TABLE sleep_data (
    id UUID PRIMARY KEY,
    day DATE,
    average_breath FLOAT,
    average_heart_rate FLOAT,
    average_hrv INTEGER,
    awake_time INTEGER,
    bedtime_start TIMESTAMP WITH TIME ZONE,
    bedtime_end TIMESTAMP WITH TIME ZONE,
    deep_sleep_duration INTEGER,
    efficiency INTEGER,
    latency INTEGER,
    light_sleep_duration INTEGER,
    lowest_heart_rate INTEGER,
    rem_sleep_duration INTEGER,
    restless_periods INTEGER,
    sleep_score_delta INTEGER,
    time_in_bed INTEGER,
    total_sleep_duration INTEGER,
    type VARCHAR(50)
);
