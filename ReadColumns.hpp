#ifndef ASU_READCOLUMNS
#define ASU_READCOLUMNS

#include<iostream>
#include<string>
#include<fstream>

/**********************************************************
 * This C++ function read from file, the file column
 * number is the second input parameter, as well as ans.size()
 *
 * input(s):
 * const std::string &filename  ----  Absolute path.
 * const std::size &nCol        ----  Number of columns.
 *
 * output(s):
 * vector<vector<double>> ans
 *                              ----  read result.
 *                                    If sucess, ans.size() should equal nCol.
 *                                    else, ans is empty.
 *
 * Shule Yu
 * Nov 16 2019
 *
 * Key words: read from file
*************************************************/

std::vector<std::vector<double>> ReadColumns(const std::string &infile, const std::size_t &nCol){
    std::ifstream fpin(infile);
    std::vector<std::vector<double>> ans(nCol);
    double x=0;
    std::size_t i=0;
    while (fpin >> x){
        ans[i].push_back(x);
        ++i;
        if (i==nCol) i=0;
    }
    return ans;
}

#endif
