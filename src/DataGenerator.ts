import { range } from 'lodash';
import { TelemetryMessage } from './main';

export function generateHourlyData(durationDays: number): TelemetryMessage[] {
  const startTime = new Date(2020, 0, 1, 0).getTime();
  return range(0, durationDays, 1 / 24).map((x: number) => {
    return {
      waterLevel: getWaterLevel(x),
      date: new Date(x * 24 * 60 * 60 * 1000 + startTime),
    };
  });
}

function getWaterLevel(x: number): number {
  return (
    makeCycle(x, -30, 365, 0, 50) + // Yearly seasonal cycle
    makeCycle(x, -0.5, 10, 0.5) + // Rainfall
    makeCycle(x, -0.15, 1, -0.5) // Daily usage
  );
}

/**
 * Execute a cyclic (cos) function on x
 * @param amplitude Amplitude of cycle
 * @param period Days to complete full cycle
 * @param trend Linear change in percentage change per day
 * @param yShift A constant which is added to move function vertically
 * @returns f(x): The output of the function
 */
function makeCycle(x: number, amplitude = 1, period = 1, trend = 0, yShift = 0): number {
  return amplitude * Math.cos(x * ((2 * Math.PI) / period)) + trend * x + yShift;
}
