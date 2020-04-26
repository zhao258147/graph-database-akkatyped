package com.example.graph

import akka.actor.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ActorRef, SupervisorStrategy}
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{Cluster, Session}
import com.example.graph.config.GraphConfig
import com.example.graph.http.{CORSHandler, RequestApi}
import com.example.graph.query.GraphActorSupervisor
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor, OffsetManagement}
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  import akka.actor.typed.scaladsl.adapter._

  val conf = ConfigFactory.load()
  val config = conf.as[GraphConfig]("GraphConfig")

  implicit val system: ActorSystem = ActorSystem("RayDemo", conf)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

  implicit val session: Session = Cluster.builder
    .addContactPoint(config.cassandraConfig.contactPoints)
    .withPort(config.cassandraConfig.port)
    .withCredentials(config.cassandraConfig.username, config.cassandraConfig.password)
    .build
    .connect()

  val offsetManagement = new OffsetManagement

  val sharding = ClusterSharding(typedSystem)

  val settings = ClusterShardingSettings(typedSystem)

  val shardRegion =
    sharding.init(Entity(GraphNodeEntity.TypeKey)(
      createBehavior = entityContext =>
        GraphNodeEntity.nodeEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
      ).withRole("graph")
    )

  val graphQuerySupervisor = system.spawn(GraphActorSupervisor.apply(shardRegion), "graphQuerySupervisor")

  val singletonManager = ClusterSingleton(typedSystem)
  val nodeReadSideActor = singletonManager.init(
    SingletonActor(
      Behaviors.supervise(
        NodeReadSideActor.ReadSideActorBehaviour(
          config.readSideConfig,
          offsetManagement
        )
      ).onFailure[Exception](SupervisorStrategy.restart), "NodeReadSide"))

  val clickReadSideActor: ActorRef[ClickReadSideActor.ClickStatCommands] = singletonManager.init(
    SingletonActor(
      Behaviors.supervise(
        ClickReadSideActor.behaviour(
          config.readSideConfig,
          offsetManagement
        )
      ).onFailure[Exception](SupervisorStrategy.restart), "ClickReadSide"))

  val route: Route = RequestApi.route(shardRegion, graphQuerySupervisor, clickReadSideActor)

  private val cors = new CORSHandler {}

  Http()(system).bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )
}
