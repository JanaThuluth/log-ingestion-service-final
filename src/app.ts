import Fastify from "fastify";

import { healthRoute } from "./routes/health";
import { logsRoutes } from "./routes/logs";

export function buildApp() {
  const app = Fastify({
    logger: true
  });

  app.register(healthRoute);
  app.register(logsRoutes);

  return app;
}