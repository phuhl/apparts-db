interface Config {
  use: string;
  postgresql: PGConfig;
}

export interface PGConfig {
  host: string;
  port: number;
  user: string;
  pw: string;
  db: string;
  maxPoolSize: number;
  connectionTimeoutMillis: number;
  idleTimeoutMillis: number;
  bigIntAsNumber: boolean;
  idsAsBigInt?: boolean;
  logs?: "errors";
  logParams?: boolean;
  arrayAsJSON?: boolean;
}

export default Config;
