export type Interval = "minute" | "hour" | "day";

export interface Source {
    name: string;
    schema: 'v1';
    provider: string;
    frequency: Interval;
    meta?: object;
}
