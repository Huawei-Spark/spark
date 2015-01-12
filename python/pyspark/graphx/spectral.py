#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os


__all__ = ['SparkFiles']


from math import *
class SpectralClustering(object):
  # A  = [[1,0,0],[0,1,0],[0,0,1]]
  A  = [[7,3],[-3,1]]
  iters = 10
  b = [.5, .5]
  # b = [0.,0.,0.]
  n = len(A)
  for i in xrange(iters):
    import array
    # tmp = array.array('f',[0.0]) * len(b)
    tmp = [0.0] * len(b)
    # tmp = [0.0, 0.0, 0.0]
    # calculate the matrix-by-vector product Ab
    for i in xrange(n):
       for j in xrange(n):
            tmp[i] += A[i][j] * b[j]
             #  dot product of i-th row in A with the column vector b

    #calculate the length of the resultant vector
    norm_sq=0
    for k in xrange(n):
         norm_sq += tmp[k]*tmp[k]
    norm = sqrt(norm_sq)

    #normalize b to unit vector for next iteration
    b = [tmp[ix]/norm for ix in xrange(n)]
  for bb in b:
    print "%d" %bb

if __name__ == '__main__':
    sp = SpectralClustering