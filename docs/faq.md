# FAQ

## Is the Shard Manager a single point of failure?

Not really. The Shard Manager is a single point of coordination, not a single point of failure. In fact, 99% of the time, the Shard Manager doesn't do anything: pods communicate with each other without going through it. It is only needed when a pod starts or stops, to reassign shards to the remaining pods.

You can safely restart the Shard Manager pod without affecting the running pods.

## I get timeouts during a rolling update, what can I do?

Start by checking the Shard Manager's logs to see what happens during the rolling update.

A common issue is that stopping pods do not unregister themselves from the Shard Manager. This can happen for several reasons:
- The pod is stopped abruptly without handling the KILL signal, so the main fiber is never interrupted.
- The pod loses network connectivity and cannot call the `unregister` endpoint (this can occur when using an Istio proxy, for example).
- The pod is stuck in a deadlock during shutdown.

In such cases, find out why the pod isn't unregistering and fix the underlying issue.

## Is Shard a replacement for Akka/Pekko?

No. Shard covers only a small portion of what Akka/Pekko does, specifically the **Cluster Sharding** feature. ZIO provides tools for local concurrency (`Queue`, `Hub`, `Promise`, etc.), and Shard adds the distributed part. Shard does not address features like event sourcing or actor persistence. If you need them, you can implement them on top of ZIO and Shard.

## What Scala and JDK versions are required?

Scala 3.9.x LTS and later, on JDK 17 or newer.

## Is Scala.js or Scala Native supported?

No. Shard is JVM-only.
