package com.example.graph

import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect, RetentionCriteria}
import com.example.graph.readside.{ClickReadSideActor, NodeReadSideActor}
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import com.lightbend.cinnamon.akka.typed.CinnamonMetrics
import com.lightbend.cinnamon.metric.{Counter, Rate, Recorder}
import org.slf4j.LoggerFactory

object GraphNodeEntity {
  //TODO: change all primitive types to value classes, so we have types for strings
  type NodeType = String
  type Tag = String
  type Weight = Int
  type TargetNodeId = String
  type NodeId = String
  type EdgeType = String
  type EdgePropertyType = String
  type EdgePropertyValue = String
  type Tags = Map[Tag, Weight]
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


  case class Edge(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, weight: Map[Tag, Weight])

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
    val replyTo: ActorRef[Reply]
  }
  case class CreateNodeCommand(nodeId: String, nodeType: NodeType, tags: Tags, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateNodeCommand(nodeId: String, tags: Tags, properties: NodeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class RemoveEdgeCommand(nodeId: String, targetNodeId: TargetNodeId, edgeType: EdgeType, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class UpdateEdgeCommand(nodeId: String, edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, userId: String, visitorLabels: Option[Map[String, Int]], replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

  case class NodeQuery(nodeId: String, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class EdgeQuery(nodeId: String, nodeType: Option[NodeType] = None, nodeProperties: NodeProperties, edgeTypes: Set[EdgeType], edgeProperties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]
  case class QueryRecommended(nodeId: String, visitorId: String, edgeTypes: Set[EdgeType], visitorLabels: Map[String, Int], edgeProperties: EdgeProperties, replyTo: ActorRef[GraphNodeCommandReply]) extends GraphNodeCommand[GraphNodeCommandReply]

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
  case class EdgeQueryResult(nodeId: NodeId, nodeType: NodeType, tags: Tags, edgeResult: Set[Edge], nodeResult: Boolean) extends GraphNodeCommandReply
  case class RecommendedResult(nodeId: NodeId, nodeType: NodeType, tags: Tags, edgeResult: Seq[Edge], clicks: Int = 0, uniqueVisitors: Int = 0) extends GraphNodeCommandReply
  case class NodeQueryResult(nodeId: NodeId, nodeType: NodeType, properties: NodeProperties, tags: Tags, edges: Set[Edge], visitors: Set[String]) extends GraphNodeCommandReply

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes(
    Array(
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeRemoved], name = "GraphNodeEdgeRemoved"),
      new JsonSubTypes.Type(value = classOf[GraphNodeUpdated], name = "GraphNodeUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeClickUpdated], name = "GraphNodeClickUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeVisitorUpdated], name = "GraphNodeVisitorUpdated"),
      new JsonSubTypes.Type(value = classOf[GraphNodeEdgeUpdated], name = "GraphNodeEdgeUpdated")))
  sealed trait GraphNodeEvent
  case class GraphNodeUpdated(id: NodeId, nodeType: NodeType, tags: Tags, properties: NodeProperties) extends GraphNodeEvent
  case class GraphNodeEdgeRemoved(targetNodeId: TargetNodeId, edgeType: EdgeType) extends  GraphNodeEvent
  case class GraphNodeEdgeUpdated(edgeType: EdgeType, direction: EdgeDirection, properties: EdgeProperties, visitorId: String, visitorLabels: Map[Tag, Weight]) extends GraphNodeEvent
  case class GraphNodeClickUpdated(nodeId: NodeId, nodeType: NodeType, ts: Long, clicks: Int) extends GraphNodeEvent
  case class GraphNodeVisitorUpdated(visitorId: String, ts: Long) extends GraphNodeEvent

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
    inEdges: EdgesWithProperties,
    tags: Tags,
    uniqueVisitors: Set[String] = Set.empty,
    activeVisitors: Map[String, Long] = Map.empty,
    clicks: Weight = 0,
    previousClicks: Int = 0,
    previousClickCommit: Long = System.currentTimeMillis()
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
                .persist(GraphNodeUpdated(create.nodeId, create.nodeType, create.tags, create.properties))
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
                .persist(GraphNodeUpdated(createdState.nodeId, createdState.nodeType, update.tags, update.properties))
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
              val visitorLabels = updateEdge.visitorLabels.getOrElse(createdState.tags.keys.map(_ -> 1)).toMap
              val evt = GraphNodeEdgeUpdated(updateEdge.edgeType, updateEdge.direction, updateEdge.properties, updateEdge.userId, visitorLabels)
              val ts = System.currentTimeMillis()
              val incrementalClicks = createdState.clicks - createdState.previousClicks
              if(ts - createdState.previousClickCommit > 60000 || incrementalClicks > 10)
                Effect
                  .persist(
                    evt,
                    GraphNodeClickUpdated(createdState.nodeId, createdState.nodeType, ts, incrementalClicks)
                  )
                  .thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))
              else Effect.persist(evt).thenReply(updateEdge.replyTo)(_ => GraphNodeCommandSuccess(updateEdge.nodeId))


            case nodeQuery: NodeQuery =>
              Effect.reply(nodeQuery.replyTo)(NodeQueryResult(createdState.nodeId, createdState.nodeType, createdState.properties, createdState.tags, createdState.outEdges.values.toList.flatMap(_.values).toSet, createdState.uniqueVisitors))

            case query: QueryRecommended =>
              println(query)
              println(createdState)

              val targetEdges: Iterable[Edge] = createdState.outEdges.values.flatMap(_.values)
              val edges = targetEdges.filter(x => query.edgeProperties.toSet.subsetOf(x.properties.toSet))

              val visitorTotalWeight = query.visitorLabels.values.sum

              val recommended = edges.foldLeft(Seq.empty[(Edge, Int)]){
                case (acc, e) =>
                  val weightTotal = query.visitorLabels.foldLeft(0){
                    case (s, (label, weight)) =>
                      s + (e.weight.getOrElse(label, 0) * (weight.toDouble / visitorTotalWeight)).toInt
                  }
                  (e -> weightTotal) +: acc
              }.sortWith(_._2 > _._2).map(_._1)

              val edgeQueryResult = RecommendedResult(createdState.nodeId, createdState.nodeType, createdState.tags, recommended, createdState.clicks, createdState.uniqueVisitors.size)
              Effect.persist(GraphNodeVisitorUpdated(query.visitorId, System.currentTimeMillis())).thenReply(query.replyTo)(_ => edgeQueryResult)

            case checkEdge: EdgeQuery =>
              println(checkEdge)
              println(createdState)
              println(checkEdge.nodeType.forall(_ == createdState.nodeType))
              println(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet))

              if(checkEdge.nodeType.forall(_ == createdState.nodeType)) {
                if(checkEdge.nodeProperties.toSet.subsetOf(createdState.properties.toSet)) {
                  val targetEdges: Edges = createdState.outEdges.filter{
                    case (edgeType, _) =>
                      checkEdge.edgeTypes.contains(edgeType) || checkEdge.edgeTypes.isEmpty
                  }.values.flatten.toMap

                  val edges: Set[Edge] = targetEdges.values.foldLeft(Set.empty[Edge]){
                    case (acc, edge: Edge) if checkEdge.edgeProperties.toSet.subsetOf(edge.properties.toSet) =>
                      edge.direction match {
                        case to: To =>
                          acc + edge
                        case _ => acc
                      }
                  }

                  val edgeQueryResult = EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, edges, true)

                  Effect.reply(checkEdge.replyTo)(edgeQueryResult)
                } else Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, Set.empty, false))
              } else
                Effect.reply(checkEdge.replyTo)(EdgeQueryResult(createdState.nodeId, createdState.nodeType, createdState.tags, Set.empty, false))

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
              inEdges = Map.empty,
              tags = created.tags
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

          case GraphNodeEdgeUpdated(edgeType, To(nodeId), properties, visitorId, labels) =>
            val weight = createdState.outEdges.get(edgeType).flatMap(_.get(nodeId).map(_.weight ++ labels)).getOrElse(labels)
            val newEdge = Edge(edgeType, To(nodeId), properties, weight)

            val targetEdges = createdState.outEdges.getOrElse(edgeType, Map.empty)
            val newTargetEdges = targetEdges + (nodeId -> newEdge)

            println(newTargetEdges)

            val newUniqueVisitors = createdState.uniqueVisitors + visitorId

            val clickrate: Rate = CinnamonMetrics(context).createRate("clickrate", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId))
            val uniquevisitorCounter: Recorder = CinnamonMetrics(context).createRecorder("uniquevisitors", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId))
            val edgesCounter: Counter = CinnamonMetrics(context).createCounter("edges", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId))
            val visitLengthRecorder: Recorder = CinnamonMetrics(context).createRecorder("visitlength", Map("nodeId" -> createdState.nodeId, "nodeType" -> createdState.nodeId))

            clickrate.mark()
            uniquevisitorCounter.record(newUniqueVisitors.size)
            if(!targetEdges.contains(nodeId)) edgesCounter.increment()
            createdState.activeVisitors.get(visitorId) foreach { ts =>
              visitLengthRecorder.record(System.currentTimeMillis() - ts)
            }

            createdState.copy(
              outEdges = createdState.outEdges + (edgeType -> newTargetEdges),
              uniqueVisitors = newUniqueVisitors,
              clicks = createdState.clicks + 1,
              activeVisitors = createdState.activeVisitors - visitorId
            )

          case GraphNodeEdgeUpdated(edgeType, From(nodeId), properties, userId, labels) =>
            val weight = createdState.inEdges.get(edgeType).flatMap(_.get(nodeId).map(_.weight ++ labels)).getOrElse(labels)
            val newEdge = Edge(edgeType, From(nodeId), properties, weight)

            val newTargetEdges = createdState.inEdges.getOrElse(edgeType, Map.empty) + (nodeId -> newEdge)

            println(newTargetEdges)

            createdState.copy(
              inEdges = createdState.outEdges + (edgeType -> newTargetEdges),
            )

          case GraphNodeClickUpdated(_, _, ts, _) =>
            createdState.copy(
              previousClicks = createdState.clicks,
              previousClickCommit = ts
            )

          case GraphNodeVisitorUpdated(visitorId, ts) =>
            createdState.copy(
              activeVisitors = createdState.activeVisitors + (visitorId -> ts)
            )
        }
    }
  }

  val TypeKey = EntityTypeKey[GraphNodeCommand[GraphNodeCommandReply]]("graph")

  def nodeEntityBehaviour(persistenceId: PersistenceId): Behavior[GraphNodeCommand[GraphNodeCommandReply]] = Behaviors.setup { context =>
    EventSourcedBehavior.withEnforcedReplies(
      persistenceId,
      EmptyGraphNodeState(),
      commandHandler(context),
      eventHandler(context)
    )
      .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
      .withTagger{
        case _: GraphNodeUpdated => Set(NodeReadSideActor.NodeUpdateEventName)
        case _: GraphNodeClickUpdated => Set(ClickReadSideActor.ClickUpdateEventName)
        case _ => Set.empty
      }
  }
}
