/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.mr3.dag;

import mr3.DAGAPI;
import mr3.api.common.MR3Conf;

public class GroupInputEdge {

  private final Vertex destVertex;
  private final EdgeProperty edgeProperty;
  private final EntityDescriptor mergedInputDescriptor;

  public GroupInputEdge(
      Vertex destVertex,
      EdgeProperty edgeProperty,
      EntityDescriptor mergedInputDescriptor) {
    this.destVertex = destVertex;
    this.edgeProperty = edgeProperty;
    this.mergedInputDescriptor = mergedInputDescriptor;
  }

  Vertex getDestVertex() {
    return destVertex;
  }

  /**
   * The EdgeProperty used for creating edges from Group Vertices to destVertex
   * @return
   */
  EdgeProperty getEdgeProperty() {
    return edgeProperty;
  }

  // DAGProto Conversion utilities
  public DAGAPI.MergedInputEdgeProto createMergedInputEdgeProto() {
    DAGAPI.MergedInputEdgeProto mergedInputEdgeProto = DAGAPI.MergedInputEdgeProto.newBuilder()
        .setDestVertexName(destVertex.getName())
        .setMergedInput(mergedInputDescriptor.createEntityDescriptorProto())
        .build();

    return mergedInputEdgeProto;
  }
}
