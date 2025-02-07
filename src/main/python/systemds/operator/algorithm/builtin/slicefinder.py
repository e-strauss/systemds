# -------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------

# Autogenerated By   : src/main/python/generator/generator.py
# Autogenerated From : scripts/builtin/slicefinder.dml

from typing import Dict, Iterable

from systemds.operator import OperationNode, Matrix, Frame, List, MultiReturn, Scalar
from systemds.utils.consts import VALID_INPUT_TYPES


def slicefinder(X: Matrix,
                e: Matrix,
                **kwargs: Dict[str, VALID_INPUT_TYPES]):
    """
     This builtin function implements SliceLine, a linear-algebra-based
     ML model debugging technique for finding the top-k data slices where
     a trained models performs significantly worse than on the overall
     dataset. For a detailed description and experimental results, see:
     Svetlana Sagadeeva, Matthias Boehm: SliceLine: Fast, Linear-Algebra-based
     Slice Finding for ML Model Debugging.(SIGMOD 2021)
    
    
    
    :param X: Feature matrix in recoded/binned representation
    :param e: Error vector of trained model
    :param k: Number of subsets required
    :param maxL: maximum level L (conjunctions of L predicates), 0 unlimited
    :param minSup: minimum support (min number of rows per slice)
    :param alpha: weight [0,1]: 0 only size, 1 only error
    :param tpEval: flag for task-parallel slice evaluation,
        otherwise data-parallel
    :param tpBlksz: block size for task-parallel execution (num slices)
    :param selFeat: flag for removing one-hot-encoded features that don't satisfy
        the initial minimum-support constraint and/or have zero error
    :param verbose: flag for verbose debug output
    :return: top-k slices (k x ncol(X) if successful)
    :return: score, total/max error, size of slices (k x 4)
    :return: debug matrix, populated with enumeration stats if verbose
    """

    params_dict = {'X': X, 'e': e}
    params_dict.update(kwargs)
    
    vX_0 = Matrix(X.sds_context, '')
    vX_1 = Matrix(X.sds_context, '')
    vX_2 = Matrix(X.sds_context, '')
    output_nodes = [vX_0, vX_1, vX_2, ]

    op = MultiReturn(X.sds_context, 'slicefinder', output_nodes, named_input_nodes=params_dict)

    vX_0._unnamed_input_nodes = [op]
    vX_1._unnamed_input_nodes = [op]
    vX_2._unnamed_input_nodes = [op]

    return op
