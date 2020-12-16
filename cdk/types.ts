export type Interval = "minute" | "hour" | "day";

export interface Source {
    schema: 'v1';
    provider: string;
    frequency: Interval;
    meta?: object;
}
