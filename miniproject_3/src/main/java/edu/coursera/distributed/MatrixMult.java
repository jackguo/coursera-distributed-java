package edu.coursera.distributed;

import edu.coursera.distributed.util.MPI;
import edu.coursera.distributed.util.MPI.MPIException;

/**
 * A wrapper class for a parallel, MPI-based matrix multiply implementation.
 */
public class MatrixMult {
    /**
     * A parallel implementation of matrix multiply using MPI to express SPMD
     * parallelism. In particular, this method should store the output of
     * multiplying the matrices a and b into the matrix c.
     *
     * This method is called simultaneously by all MPI ranks in a running MPI
     * program. For simplicity MPI_Init has already been called, and
     * MPI_Finalize should not be called in parallelMatrixMultiply.
     *
     * On entry to parallelMatrixMultiply, the following will be true of a, b,
     * and c:
     *
     *   1) The matrix a will only be filled with the input values on MPI rank
     *      zero. Matrix a on all other ranks will be empty (initialized to all
     *      zeros).
     *   2) Likewise, the matrix b will only be filled with input values on MPI
     *      rank zero. Matrix b on all other ranks will be empty (initialized to
     *      all zeros).
     *   3) Matrix c will be initialized to all zeros on all ranks.
     *
     * Upon returning from parallelMatrixMultiply, the following must be true:
     *
     *   1) On rank zero, matrix c must be filled with the final output of the
     *      full matrix multiplication. The contents of matrix c on all other
     *      ranks are ignored.
     *
     * Therefore, it is the responsibility of this method to distribute the
     * input data in a and b across all MPI ranks for maximal parallelism,
     * perform the matrix multiply in parallel, and finally collect the output
     * data in c from all ranks back to the zeroth rank. You may use any of the
     * MPI APIs provided in the mpi object to accomplish this.
     *
     * A reference sequential implementation is provided below, demonstrating
     * the use of the Matrix class's APIs.
     *
     * @param a Input matrix
     * @param b Input matrix
     * @param c Output matrix
     * @param mpi MPI object supporting MPI APIs
     * @throws MPIException On MPI error. It is not expected that your
     *                      implementation should throw any MPI errors during
     *                      normal operation.
     */
    public static void parallelMatrixMultiply(Matrix a, Matrix b, Matrix c,
            final MPI mpi) throws MPIException {
        int myRank = mpi.MPI_Comm_rank(mpi.MPI_COMM_WORLD);
        int totalRanks = mpi.MPI_Comm_size(mpi.MPI_COMM_WORLD);
        mpi.MPI_Bcast(b.getValues(), 0, b.getValues().length, 0, mpi.MPI_COMM_WORLD);
        //mpi.MPI_Bcast(a.getValues(), 0, a.getValues().length, 0, mpi.MPI_COMM_WORLD);

        int myRows = calculateMyRows(a.getNRows(), myRank, totalRanks);
        int myFirstRow = 0;
        MPI.MPI_Request[] sendRequests = new MPI.MPI_Request[totalRanks - 1];
        if (myRank == 0) {
            for (int i = 0; i < totalRanks - 1; ++i) {
                sendRequests[i] = mpi.MPI_Isend(a.getValues(),
                        (myRows + a.getNRows() / totalRanks * i) * a.getNCols(),
                        a.getNRows() / totalRanks * a.getNCols(),
                        i + 1,
                        0,
                        mpi.MPI_COMM_WORLD);
            }
        } else {
            myFirstRow = (calculateMyRows(a.getNRows(), 0, totalRanks) + a.getNRows() / totalRanks * (myRank - 1));
            mpi.MPI_Recv(a.getValues(), myFirstRow * a.getNCols(), a.getNRows() / totalRanks * a.getNCols(), 0, -0, mpi.MPI_COMM_WORLD);
        }

        if (myRank == 0) {
            mpi.MPI_Waitall(sendRequests);
        }

        for (int i = myFirstRow; i < myFirstRow + myRows; ++i) {
            for (int j = 0; j < c.getNCols(); ++j) {
                c.set(i, j, 0.0);

                for (int k = 0; k < b.getNRows(); k++) {
                    c.incr(i, j, a.get(i, k) * b.get(k, j));
                }
            }
        }

        if (myRank == 0) {
            MPI.MPI_Request[] recvReq = new MPI.MPI_Request[totalRanks - 1];
            for (int i = 0; i < recvReq.length; ++i) {
                recvReq[i] = mpi.MPI_Irecv(c.getValues(),
                        (myRows + c.getNRows() / totalRanks * i) * c.getNCols(),
                        c.getNRows() / totalRanks * c.getNCols(),
                        i + 1,
                        0,
                        mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Waitall(recvReq);
        } else {
            mpi.MPI_Send(c.getValues(), myFirstRow * c.getNCols(), c.getNRows() / totalRanks * c.getNCols(), 0, -0, mpi.MPI_COMM_WORLD );
        }
    }

    private static int calculateMyRows(int total, int myRank, int worldSize) {
        int avg = total / worldSize;

        if (myRank == 0) {
            return avg + total % worldSize;
        } else {
            return avg;
        }
    }
}
