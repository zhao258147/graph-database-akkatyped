package com.example.personalization

import akka.actor.ActorSystem
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.cluster.typed.{ClusterSingleton, SingletonActor}
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.management.scaladsl.AkkaManagement
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{Cluster, Session}
import com.example.graph.GraphNodeEntity
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply}
import com.example.graph.config.GraphConfig
import com.example.graph.http.{CORSHandler, RequestApi}
import com.example.graph.query.GraphActorSupervisor
import com.example.graph.readside.NodeReadSideActor
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  import akka.actor.typed.scaladsl.adapter._

  val conf = ConfigFactory.load()
  implicit val system: ActorSystem = ActorSystem("RayDemo", conf)
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  implicit val typedSystem: akka.actor.typed.ActorSystem[Nothing] = system.toTyped

  implicit val session: Session = Cluster.builder
    .addContactPoint("127.0.0.1")
    .withPort(9042)
    .build
    .connect()

  val sharding = ClusterSharding(typedSystem)

  val config = conf.as[GraphConfig]("GraphConfig")

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

  val route: Route = RequestApi.route(shardRegion, graphQuerySupervisor)

  private val cors = new CORSHandler {}

  Http()(system).bindAndHandle(
    cors.corsHandler(route),
    config.http.interface,
    config.http.port
  )

  val singletonManager = ClusterSingleton(typedSystem)
  val readSideActor = singletonManager.init(
    SingletonActor(Behaviors.supervise(NodeReadSideActor.ReadSideActorBehaviour(config.readSideConfig)).onFailure[Exception](SupervisorStrategy.restart), "ReadSide"))
}
