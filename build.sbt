val scala3 = "3.9.0"

val zioVersion            = "2.1.24"
val proteusVersion        = "0.7.0"
val grpcNettyVersion      = "1.85.1"
val zioK8sVersion         = "3.2.0"
val zioK8sSttpVersion     = "3.11.0"
val zioCacheVersion       = "0.2.4"
val zioCatsInteropVersion = "23.1.0.5"
val zioJsonVersion        = "0.7.39"
val redis4catsVersion     = "2.0.1"
val redissonVersion       = "3.45.1"
val scalaKryoVersion      = "1.4.0"
val testContainersVersion = "0.44.1"

inThisBuild(
  List(
    scalaVersion := scala3,
    organization := "com.github.ghostdogpr",
    homepage     := Some(url("https://ghostdogpr.github.io/shard/")),
    licenses     := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    scmInfo      := Some(
      ScmInfo(
        url("https://github.com/ghostdogpr/shard"),
        "scm:git:git@github.com:ghostdogpr/shard.git"
      )
    ),
    developers   := List(
      Developer(
        "ghostdogpr",
        "Pierre Ricadat",
        "ghostdogpr@gmail.com",
        url("https://github.com/ghostdogpr")
      )
    )
  )
)

addCommandAlias("fmt", "all scalafmtSbt scalafmt Test/scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck Test/scalafmtCheck grpcProtocol/checkProto")

lazy val root = project
  .in(file("."))
  .settings(name := "shard", publish / skip := true)
  .aggregate(
    core,
    manager,
    entities,
    healthK8s,
    storageRedis,
    storageRedisson,
    serializationKryo,
    serializationProteus,
    grpcProtocol,
    examples,
    benchmarks
  )

lazy val core = project
  .in(file("core"))
  .settings(name := "shard-core")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++=
      Seq(
        "dev.zio" %% "zio"         % zioVersion,
        "dev.zio" %% "zio-streams" % zioVersion,
        "dev.zio" %% "zio-json"    % zioJsonVersion
      )
  )

lazy val manager = project
  .in(file("manager"))
  .settings(name := "shard-manager")
  .settings(commonSettings)
  .dependsOn(core, grpcProtocol)

lazy val entities = project
  .in(file("entities"))
  .settings(name := "shard-entities")
  .settings(commonSettings)
  .dependsOn(core)

lazy val healthK8s = project
  .in(file("health-k8s"))
  .settings(name := "shard-health-k8s")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies ++=
      Seq(
        "com.coralogix"                 %% "zio-k8s-client" % zioK8sVersion,
        "dev.zio"                       %% "zio-cache"      % zioCacheVersion,
        "com.softwaremill.sttp.client3" %% "zio"            % zioK8sSttpVersion,
        "com.softwaremill.sttp.client3" %% "slf4j-backend"  % zioK8sSttpVersion
      )
  )

lazy val storageRedis = project
  .in(file("storage-redis"))
  .settings(name := "shard-storage-redis")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies ++=
      Seq(
        "dev.profunktor" %% "redis4cats-effects" % redis4catsVersion,
        "dev.profunktor" %% "redis4cats-streams" % redis4catsVersion,
        "dev.zio"        %% "zio-interop-cats"   % zioCatsInteropVersion
      )
  )

lazy val storageRedisson = project
  .in(file("storage-redisson"))
  .settings(name := "shard-storage-redisson")
  .settings(commonSettings)
  .dependsOn(core)
  .settings(
    libraryDependencies ++=
      Seq(
        "org.redisson" % "redisson" % redissonVersion
      )
  )

lazy val serializationKryo = project
  .in(file("serialization-kryo"))
  .settings(name := "shard-serialization-kryo")
  .settings(commonSettings)
  .dependsOn(entities)
  .settings(
    libraryDependencies ++=
      Seq(
        "io.altoo" %% "scala-kryo-serialization" % scalaKryoVersion
      )
  )

lazy val generateProto = taskKey[Unit]("Regenerate the .proto files from the Scala protocol definitions.")
lazy val checkProto    = taskKey[Unit]("Fail if the .proto files are out of sync with the Scala protocol definitions.")

lazy val serializationProteus = project
  .in(file("serialization-proteus"))
  .settings(name := "shard-serialization-proteus")
  .settings(commonSettings)
  .dependsOn(entities)
  .settings(
    libraryDependencies ++=
      Seq(
        "com.github.ghostdogpr" %% "proteus-core" % proteusVersion
      )
  )

lazy val grpcProtocol = project
  .in(file("protocol-grpc"))
  .settings(name := "shard-protocol-grpc")
  .settings(commonSettings)
  .dependsOn(core, entities)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.ghostdogpr" %% "proteus-grpc"     % proteusVersion,
      "com.github.ghostdogpr" %% "proteus-grpc-zio" % proteusVersion,
      "io.grpc"                % "grpc-netty"       % grpcNettyVersion,
      "io.grpc"                % "grpc-services"    % grpcNettyVersion
    ),
    generateProto := Def.uncached {
      val cp     = (Compile / fullClasspath).value
      val conv   = fileConverter.value
      val log    = streams.value.log
      val output = (Compile / sourceDirectory).value / "protobuf"
      runner.value
        .run(
          "shard.protocol.GenerateProto",
          cp.map(a => conv.toPath(a.data)),
          Seq(output.getAbsolutePath),
          log
        )
        .get
      log.info(s"Regenerated .proto files in $output")
    },
    checkProto    := Def.uncached {
      val log  = streams.value.log
      val dir  = (Compile / sourceDirectory).value / "protobuf"
      val _    = generateProto.value
      import scala.sys.process._
      val diff = s"git diff --exit-code -- ${dir.getAbsolutePath}".!
      if (diff != 0) {
        sys.error(
          "The .proto files are out of sync with the Scala protocol definitions. Run `sbt grpcProtocol/generateProto` and commit the result."
        )
      } else log.info("The .proto files are in sync.")
    }
  )

lazy val examples = project
  .in(file("examples"))
  .settings(name := "examples")
  .settings(publish / skip := true)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++=
      Seq(
        "dev.zio" %% "zio"         % zioVersion,
        "dev.zio" %% "zio-streams" % zioVersion
      )
  )
  .dependsOn(manager, storageRedis, grpcProtocol, serializationKryo)

lazy val benchmarks = project
  .in(file("benchmarks"))
  .settings(name := "benchmarks")
  .settings(publish / skip := true)
  .settings(commonSettings)
  .enablePlugins(JmhPlugin)
  .dependsOn(grpcProtocol, serializationKryo)

lazy val commonSettings = Def.settings(
  testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework")),
  libraryDependencies ++=
    Seq(
      "dev.zio"      %% "zio-test"                  % zioVersion            % Test,
      "dev.zio"      %% "zio-test-sbt"              % zioVersion            % Test,
      "com.dimafeng" %% "testcontainers-scala-core" % testContainersVersion % Test
    ),
  Test / fork    := true,
  Test / javaOptions ++= Seq(
    // Kryo requires this with the recent versions of Java
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.sql/java.sql=ALL-UNNAMED"
  ),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:higherKinds",
    "-language:existentials",
    "-unchecked",
    "-Werror",
    "-language:postfixOps",
    "-explain-types",
    "-Xkind-projector"
  )
)
