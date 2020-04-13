package com.example.saga

import java.util.UUID

import akka.actor.typed.{ActorRef, ActorSystem, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.{ClusterShardingSettings, ShardingEnvelope}
import akka.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import akka.persistence.typed.PersistenceId
import akka.stream.ActorMaterializer
import com.example.graph.GraphNodeEntity
import com.example.graph.GraphNodeEntity.{GraphNodeCommand, GraphNodeCommandReply, NodeId, TargetNodeId}
import com.example.graph.config.GraphConfig
import com.example.personalization.Main.{conf, typedSystem}
import com.example.saga.SagaActor.{NodeReferral, SagaActorReply}
import com.example.saga.config.SagaConfig
import com.example.user.Main.sharding
import com.example.user.UserNodeEntity
import com.example.user.UserNodeEntity.{UserCommand, UserId, UserReply}
import com.typesafe.config.ConfigFactory
import net.ceedubs.ficus.Ficus._
import net.ceedubs.ficus.readers.ArbitraryTypeReader._

import scala.concurrent.ExecutionContextExecutor

object Main extends App {
  val conf = ConfigFactory.load()
  val config = conf.as[SagaConfig]("SagaConfig")

  import akka.actor.typed.scaladsl.adapter._

  implicit val typedSystem: ActorSystem[SagaCommand] = ActorSystem(Main(), "RayDemo")

//  implicit val system: ActorSystem = ActorSystem("RayDemo", conf)
//  implicit val materializer: ActorMaterializer = ActorMaterializer()
//  implicit val ec: ExecutionContextExecutor = typedSystem.dispatcher



  val sharding = ClusterSharding(typedSystem)

  val settings = ClusterShardingSettings(typedSystem)

  val GraphTypeKey = EntityTypeKey[GraphNodeCommand[GraphNodeCommandReply]]("graph")

  val graphShardRegion: ActorRef[ShardingEnvelope[GraphNodeCommand[GraphNodeCommandReply]]] =
    sharding.init(Entity(GraphTypeKey)(
      createBehavior = entityContext =>
        GraphNodeEntity.nodeEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ).withRole("graph"))


  val UserTypeKey = EntityTypeKey[UserCommand[UserReply]]("user")

  val userShardRegion: ActorRef[ShardingEnvelope[UserCommand[UserReply]]] =
    sharding.init(Entity(UserTypeKey)(
      createBehavior = entityContext =>
        UserNodeEntity.userEntityBehaviour(
          PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)
        )
    ).withRole("user"))


  sealed trait SagaCommand
  case class NodeReferralCommand(nodeId: NodeId, targetNodeId: TargetNodeId, userId: UserId, userLabels: Set[String], replyTo: ActorRef[SagaActorReply]) extends SagaCommand

  def apply(): Behavior[SagaCommand] = Behaviors.setup{ context =>
    Behaviors.receiveMessage{
      case referral: NodeReferralCommand =>
        val sagaActor = context.spawn(SagaActor(graphShardRegion, userShardRegion), UUID.randomUUID().toString)
        sagaActor ! NodeReferral(referral.nodeId, referral.targetNodeId, referral.userId, referral.userLabels, referral.replyTo)
        Behaviors.same
    }
  }

}
