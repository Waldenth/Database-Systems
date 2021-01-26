//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// starter_test.cpp
//
// Identification: test/include/starter_test.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>

#include "gtest/gtest.h"
#include "primer/p0_starter.h"

namespace bustub {

TEST(StarterTest, SampleTest) {
  int a = 1;
  EXPECT_EQ(a, 1);
}

TEST(StarterTest, AddMatricesTest) {
  // normal add Matrix<int>
  {
    std::unique_ptr<RowMatrix<int>> mat1_ptr{new RowMatrix<int>(3, 3)};
    int arr1[9] = {1, 4, 2, 5, 2, -1, 0, 3, 1};
    mat1_ptr->MatImport(&arr1[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr1[i * 3 + j], mat1_ptr->GetElem(i, j));
      }
    }

    int arr2[9] = {2, -3, 1, 4, 6, 7, 0, 5, -2};
    std::unique_ptr<RowMatrix<int>> mat2_ptr{new RowMatrix<int>(3, 3)};
    mat2_ptr->MatImport(&arr2[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr2[i * 3 + j], mat2_ptr->GetElem(i, j));
      }
    }

    int arr3[9] = {3, 1, 3, 9, 8, 6, 0, 8, -1};
    std::unique_ptr<RowMatrix<int>> sum_ptr = RowMatrixOperations<int>::AddMatrices(move(mat1_ptr), move(mat2_ptr));
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr3[i * 3 + j], sum_ptr->GetElem(i, j));
      }
    }
  }

  // dimension not match
  {
    std::unique_ptr<RowMatrix<int>> mat1_ptr{new RowMatrix<int>(1, 1)};
    std::unique_ptr<RowMatrix<int>> mat2_ptr{new RowMatrix<int>(2, 2)};
    std::unique_ptr<RowMatrix<int>> null_ptr = RowMatrixOperations<int>::AddMatrices(move(mat1_ptr), move(mat2_ptr));
    EXPECT_EQ(nullptr, null_ptr.get());
  }

  // normal add Matrix<double>
  {
    std::unique_ptr<RowMatrix<double>> mat1_ptr{new RowMatrix<double>(3, 3)};
    double arr1[9] = {1.0, 4.0, 2.0, 5.0, 2.0, -1.0, 0.0, 3.0, 1.0};
    mat1_ptr->MatImport(&arr1[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr1[i * 3 + j], mat1_ptr->GetElem(i, j));
      }
    }

    double arr2[9] = {2.0, -3.0, 1.0, 4.0, 6.0, 7.0, 0.0, 5.0, -2.0};
    std::unique_ptr<RowMatrix<double>> mat2_ptr{new RowMatrix<double>(3, 3)};
    mat2_ptr->MatImport(&arr2[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr2[i * 3 + j], mat2_ptr->GetElem(i, j));
      }
    }

    double arr3[9] = {3.0, 1.0, 3.0, 9.0, 8.0, 6.0, 0.0, 8.0, -1.0};
    std::unique_ptr<RowMatrix<double>> sum_ptr =
        RowMatrixOperations<double>::AddMatrices(move(mat1_ptr), move(mat2_ptr));
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr3[i * 3 + j], sum_ptr->GetElem(i, j));
      }
    }
  }
}

TEST(StarterTest, MultiplyMatricesTest) {
  // normal multiply Matrix<int>
  {
    int arr1[6] = {1, 2, 3, 4, 5, 6};
    std::unique_ptr<RowMatrix<int>> mat1_ptr{new RowMatrix<int>(2, 3)};
    mat1_ptr->MatImport(&arr1[0]);
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr1[i * 3 + j], mat1_ptr->GetElem(i, j));
      }
    }

    int arr2[6] = {-2, 1, -2, 2, 2, 3};
    std::unique_ptr<RowMatrix<int>> mat2_ptr{new RowMatrix<int>(3, 2)};
    mat2_ptr->MatImport(&arr2[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr2[i * 2 + j], mat2_ptr->GetElem(i, j));
      }
    }

    int arr3[4] = {0, 14, -6, 32};
    std::unique_ptr<RowMatrix<int>> product_ptr =
        RowMatrixOperations<int>::MultiplyMatrices(move(mat1_ptr), move(mat2_ptr));
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr3[i * 2 + j], product_ptr->GetElem(i, j));
      }
    }
  }

  // dimension not match
  {
    std::unique_ptr<RowMatrix<int>> mat1_ptr{new RowMatrix<int>(1, 1)};
    std::unique_ptr<RowMatrix<int>> mat2_ptr{new RowMatrix<int>(2, 2)};
    std::unique_ptr<RowMatrix<int>> null_ptr =
        RowMatrixOperations<int>::MultiplyMatrices(move(mat1_ptr), move(mat2_ptr));
    EXPECT_EQ(nullptr, null_ptr.get());
  }

  // normal multiply Matrix<double>
  {
    double arr1[6] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    std::unique_ptr<RowMatrix<double>> mat1_ptr{new RowMatrix<double>(2, 3)};
    mat1_ptr->MatImport(&arr1[0]);
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr1[i * 3 + j], mat1_ptr->GetElem(i, j));
      }
    }

    double arr2[6] = {-2.0, 1.0, -2.0, 2.0, 2.0, 3.0};
    std::unique_ptr<RowMatrix<double>> mat2_ptr{new RowMatrix<double>(3, 2)};
    mat2_ptr->MatImport(&arr2[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr2[i * 2 + j], mat2_ptr->GetElem(i, j));
      }
    }

    double arr3[4] = {0.0, 14.0, -6.0, 32.0};
    std::unique_ptr<RowMatrix<double>> product_ptr =
        RowMatrixOperations<double>::MultiplyMatrices(move(mat1_ptr), move(mat2_ptr));
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr3[i * 2 + j], product_ptr->GetElem(i, j));
      }
    }
  }
}

TEST(StarterTest, GemmMatricesTest) {
  // normal gemm Matrix<int>
  {
    int arr1[6] = {1, 2, 3, 4, 5, 6};
    std::unique_ptr<RowMatrix<int>> matA_ptr{new RowMatrix<int>(2, 3)};
    matA_ptr->MatImport(&arr1[0]);
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 3; j++) {
        EXPECT_EQ(arr1[i * 3 + j], matA_ptr->GetElem(i, j));
      }
    }

    int arr2[6] = {-2, 1, -2, 2, 2, 3};
    std::unique_ptr<RowMatrix<int>> matB_ptr{new RowMatrix<int>(3, 2)};
    matB_ptr->MatImport(&arr2[0]);
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr2[i * 2 + j], matB_ptr->GetElem(i, j));
      }
    }

    int arr3[4] = {0, 14, -6, 32};
    std::unique_ptr<RowMatrix<int>> matC_ptr{new RowMatrix<int>(2, 2)};
    matC_ptr->MatImport(&arr3[0]);
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr3[i * 2 + j], matC_ptr->GetElem(i, j));
      }
    }

    int arr4[4] = {0, 28, -12, 64};
    std::unique_ptr<RowMatrix<int>> product_ptr =
        RowMatrixOperations<int>::GemmMatrices(move(matA_ptr), move(matB_ptr), move(matC_ptr));
    for (int i = 0; i < 2; i++) {
      for (int j = 0; j < 2; j++) {
        EXPECT_EQ(arr4[i * 2 + j], product_ptr->GetElem(i, j));
      }
    }
  }

  // dimension not match
  {
    std::unique_ptr<RowMatrix<int>> matA_ptr{new RowMatrix<int>(1, 1)};
    std::unique_ptr<RowMatrix<int>> matB_ptr{new RowMatrix<int>(2, 2)};
    std::unique_ptr<RowMatrix<int>> matC_ptr{new RowMatrix<int>(2, 2)};
    std::unique_ptr<RowMatrix<int>> null_ptr =
        RowMatrixOperations<int>::GemmMatrices(move(matA_ptr), move(matB_ptr), move(matC_ptr));
    EXPECT_EQ(nullptr, null_ptr.get());
  }
}
}  // namespace bustub
