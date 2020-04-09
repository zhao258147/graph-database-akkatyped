package com.example.graph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.example.graph.readside.ReadSideActor
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import org.slf4j.LoggerFactory

object GraphNodeEntity {
  type NodeType = String
  type TargetNodeId = String
  type NodeId = String
  type EdgeType = String
  type EdgePropertyType = String
  type EdgePropertyValue = String
  type EdgeProperties = Map[EdgePropertyType, EdgePropertyValue]
  type Edges = Map[TargetNodeId, Edge]
  type EdgesWithProperties = Map[EdgeType, Edges]
  type NodeProperties = Map[String, String]

  implicit val logger =
    LoggerFactory.getLogger(classOf[CreatedGraphNodeState])

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[To], name = "To"),
      new JsonSubTypes.Type(value = classOf[From], name = "From")
    ))
  sealed trait EdgeDirection{
    val nodeId: String
  }
  case class To(nodeId: String) extends EdgeDirection
  case class From(nodeId: String) extends EdgeDirection


  case class Edge(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, weight: Int = 0)

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[CreateNodeCommand], name = "CreateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[UpdateNodeCommand], name = "UpdateNodeCommand"),
      new JsonSubTypes.Type(value = classOf[RemoveEdgeCommand], name = "RemoveEdge"),
      new JsonSubTypes.Type(value = classOf[EdgeQuery], name = "EdgeQuery"),
      new JsonSubTypes.Type(value = classOf[UpdateEdgeCommand], name = "UpdateEdge")))
  sealed trait GraphNodeCommand[Reply <: GraphNodeCommandReply] {
    val nodeId: NodeId
    def replyTo: ActorRef[Reply]
  }
  case class CreateNodeCommand(nodeId: String, nodeType: NodeType, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateNodeCommand(nodeId: String, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class RemoveEdgeCommand(nodeId: String, targetNodeId: TargetNodeId, edgeType: EdgeType, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateEdgeCommand(nodeId: String, edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

  case class NodeQuery(nodeId: String, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class EdgeQuery(nodeId: String, nodeType: Option[NodeType], nodeProperties: NodeProperties, edgeType: Option[EdgeType], edgeProperties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandSuccess], name = "GraphNodeCommandSuccess"),
      new JsonSubTypes.Type(value = classOf[GraphNodeCommandFailed], name = "GraphNodeCommandFailed"),
      new JsonSubTypes.Type(value = classOf[EdgeQueryResult], name = "EdgeQueryResult")))
  sealed trait GraphNodeCommandReply {
    val nodeId: NodeId
  }
  case class GraphNodeCommandSuccess(nodeId: NodeId, message: String = "") extends GraphNodeCommandReply
  case class GraphNodeCommandFailed(nodeId: NodeId, error: String) extends GraphNodeCommandReply
  case class EdgeQueryResult(nodeId: NodeId, edgeResult: Set[Edge], nodeResult: Boolean) extends GraphNodeCommandReply
  case class NodeQueryResult(nodeId: NodeId, nodeType: NodeType, properties: NodeProperties) extends GraphNodeCommandReply

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeRemoved], name = "GraphNodeEdgeRemoved"),
      new JsonSubTypes.Type(value = classOf[GraphNodeUpdated], name = "GraphNodeUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeUpdated], name = "GraphNodeEdgeUpdated")))
  sealed trait GraphNodeEvent
  case class GraphNodeUpdated(id: NodeId, nodeType: NodeType, properties: NodeProperties) extends GraphNodeEvent
  case class GraphNodeEdgeRemoved(targetNodeId: TargetNodeId, edgeType: EdgeType) extends  GraphNodeEvent
  case class GraphNodeEdgeUpdated(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties) extends GraphNodeEvent

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[EmptyGraphNodeState], name = "EmptyGraphNodeState"),
      new JsonSubTypes.Type(value = classOf[CreatedGraphNodeState], name = "CreatedGraphNodeState")))
  sealed trait GraphNodeState
  case class EmptyGraphNodeState() extends GraphNodeState
  case class CreatedGraphNodeState(
    nodeId: NodeId,
    nodeType: NodeType,
    properties: NodeProperties,
    outEdges: EdgesWithProperties,
    inEdges: EdgesWithProperties
  ) extends GraphNodeState

  private def commandHandler(context: ActorContext[GraphNodeCommand[GraphNodeCommandReply]]):
  (GraphNodeState, GraphNodeCommand[GraphNodeCommandReply]) => ReplyEffect[GraphNodeEvent, GraphNodeState] = {
    (state, command) =>
      println("command " * 10)

      println(command)

      logger.info(s"$state")

      state match {
        case _: EmptyGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(create.nodeId, create.nodeType, create.properties))
                .thenReply(create.replyTo)(_ => GraphNodeCommandSuccess(create.nodeId))
            case cmd =>
              Effect.reply(cmd.replyTo)(GraphNodeCommandFailed(cmd.nodeId, "Node does not exist"))
          }
        case createdState: CreatedGraphNodeState =>
          command match {
            case create: CreateNodeCommand =>
              Effect.reply(create.replyTo)(GraphNodeCommandFailed(create.nodeId, "Node exists"))

            case update: UpdateNodeCommand =>
              Effect
                .persist(GraphNodeUpdated(createdState.nodeId, createdState.nodeType, update.properties))
                .thenReply(update.replyTo)(_ => GraphNodeCommandSuccess(update.nodeId))

            case removeEdge: RemoveEdgeCommand =>
              val existingEdge = for {
                targetEdges <- createdState.outEdges.get(removeEdge.edgeType)
                edges <- targetEdges.get(removeEdge.targetNodeId)
              } yield edges

              existingEdge match {
                case Some(_) =>
                  Effect
                    .persist(GraphNodeEdgeRemoved(removeEdge.targetNodeId, removeEdge.edgeType))
                    .thenReply(removeEdge.replyTo)(_ => GraphNodeCommandSuccess(removeEdge.nodeId))
                case _ =>
                  Effect.reply(removeEdge.replyTo)(GraphNodeCommandFailed(removeEdge.nodeId, "Edge does not exist"))
              }

            case updateEdge: UpdateEdgeCommand =>
              Effect
                .persist(GraphNodeEdgeUpdated(updateEdge.edgeType, updateEdge.direction, updateEdge.properties))
                .thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))

            case nodeQuery: NodeQuery =>
              Effect.reply(nodeQuery.replyTo)(NodeQueryResult(createdState.nodeId, createdState.nodeType, createdState.properties))

            case checkEdge: EdgeQuery =>
              println(checkEdge)
              println(createdState)
              println(checkEdge.nodeType.forall(_ == createdState.nodeType))
              println(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet))

              if(checkEdge.nodeType.forall(_ == createdState.nodeType)) {
                if(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet)) {
                  val targetEdges: Edges = createdState.outEdges.values.flatten.toMap

                  val edges: List[Edge] = targetEdges.values.foldLeft(List.empty[Edge]){
                    case (acc, edge: Edge) if checkEdge.edgeProperties.toSet.subsetOf(edge.properties.toSet) =>
                      edge.direction match {
                        case to: To =>
                          edge +: acc
                        case _ => acc
                      }
                  }.sortWith(_.weight > _.weight)

                  Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, edges.toSet, true))
                } else Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, Set.empty, false))
              } else
                Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, Set.empty, false))

          }
      }
  }

  private def eventHandler(context: ActorContext[GraphNodeCommand[GraphNodeCommandReply]]): (GraphNodeState, GraphNodeEvent) => GraphNodeState = { (state, event) =>
    println("state" * 10)
    println(state)
    state match {
      case _: EmptyGraphNodeState =>
        event match {
          case created: GraphNodeUpdated =>
            CreatedGraphNodeState(
              nodeId = created.id,
              properties = created.properties,
              nodeType = created.nodeType,
              outEdges = Map.empty,
              inEdges = Map.empty
            )
          case _ =>
            state
        }
      case createdState: CreatedGraphNodeState =>
        event match {
          case updated: GraphNodeUpdated =>
            createdState.copy(
              nodeId = updated.id,
              properties = updated.properties,
              nodeType = updated.nodeType
            )

          case removeEdge: GraphNodeEdgeRemoved =>
            val targetEdges = createdState.outEdges.getOrElse(removeEdge.edgeType, Map.empty)
            val newEdges = targetEdges - removeEdge.targetNodeId
            if(newEdges.isEmpty)
              createdState.copy(outEdges = createdState.outEdges - removeEdge.edgeType)
            else
              createdState.copy(outEdges = createdState.outEdges + (removeEdge.edgeType -> newEdges))

          case GraphNodeEdgeUpdated(edgeType, To(nodeId), properties) =>
            val weight = createdState.outEdges.get(edgeType).flatMap(_.get(nodeId).map(_.weight)).getOrElse(0)
            val newEdge = Edge(edgeType, To(nodeId), properties, weight + 1)

            val newTargetEdges = createdState.outEdges.getOrElse(edgeType, Map.empty) + (nodeId -> newEdge)

            println(newTargetEdges)

            createdState.copy(
              outEdges = createdState.outEdges + (edgeType -> newTargetEdges),
            )

          case GraphNodeEdgeUpdated(edgeType, From(nodeId), properties) =>
            val newEdge = Edge(edgeType, From(nodeId), properties)

            val newTargetEdges = createdState.inEdges.getOrElse(edgeType, Map.empty) + (nodeId -> newEdge)

            println(newTargetEdges)

            createdState.copy(
              inEdges = createdState.outEdges + (edgeType -> newTargetEdges),
            )
        }
    }
  }

  def nodeEntityBehaviour(persistenceId: PersistenceId): Behavior[GraphNodeCommand[GraphNodeCommandReply]] = Behaviors.setup { context =>
    EventSourcedBehavior.withEnforcedReplies(
      persistenceId,
      EmptyGraphNodeState(),
      commandHandler(context),
      eventHandler(context)
    )
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
      .withTagger{
        case _: GraphNodeUpdated => Set(ReadSideActor.NodeUpdateEventName)
        case _ => Set.empty
      }
  }
}
