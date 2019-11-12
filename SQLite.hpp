#ifndef ASU_SQLITE
#define ASU_SQLITE

#include<iostream>
#include<cstdlib>
#include<cmath>
#include<algorithm>
#include<vector>
#include<map>
#include<string>
#include<tuple>
#include<unistd.h>

#include<sqlite3.h>

#include<SortWithIndex.hpp>
#include<ReorderUseIndex.hpp>

/*************************************************
 * This is a wrapper for sqlite3 API.
 *
 * Shule Yu
 * Nov 30 2019
 *
 * Key words: sqlite 3, mysql
*************************************************/

namespace SQLite {

    const int ISINT=0,ISSTRING=1,ISFLOAT=2;

    class Select {

        std::size_t m,n;
        std::map<std::string,int> NameToIndex,TypeCheck;
        std::vector<std::vector<int>> PI;
        std::vector<std::vector<std::string>> PS;
        std::vector<std::vector<double>> PF;


    public:

        const std::vector<int> &GetInt(const std::string &s) const {
            if (NameToIndex.find(s)==NameToIndex.end())
                throw std::runtime_error("Field "+s+" does not exist ...");
            if (TypeCheck.at(s)!=SQLite::ISINT)
                throw std::runtime_error("Field "+s+" is not integer ...");
            return PI[NameToIndex.at(s)];
        }

        const std::vector<std::string> &GetString(const std::string &s) const {
            if (NameToIndex.find(s)==NameToIndex.end())
                throw std::runtime_error("Field "+s+" does not exist ...");
            if (TypeCheck.at(s)!=SQLite::ISSTRING)
                throw std::runtime_error("Field "+s+" is not string ...");
            return PS[NameToIndex.at(s)];
        }

        const std::vector<double> &GetDouble(const std::string &s) const{
            if (NameToIndex.find(s)==NameToIndex.end())
                throw std::runtime_error("Field "+s+" does not exist ...");
            if (TypeCheck.at(s)!=SQLite::ISFLOAT)
                throw std::runtime_error("Field "+s+" is not float ...");
            return PF[NameToIndex.at(s)];
        }

        void Clear() {
            PI.clear();
            PS.clear();
            PF.clear();
            NameToIndex.clear();
            TypeCheck.clear();
        }

        const std::size_t &NCol() const {return n;}
        const std::size_t &NRow() const {return m;}

        Select()=default;
        Select (const Select &item) = default;
        Select(const std::string &dbFile, const std::string &cmd) {


            /*
               Because SQLite3 use affinity data type (type definition is not strict)
               Some conventions in this API:
               (Still buggy)
              
                  1. The data type (integer, double, string) of each column (in the Select result)
                     will be decided as the affinity data type interpreted from the 1st row.

                  2. For the following rows, their affinity data type are compared to the 1st row,
                     actions are taken if the data type doesn't match:
              
              
                      1st row datatype      this row affinity          actions
              
                      int                   numeric                    set value to std::numeric_limits<int>::max()
                      int                   text                       set value to std::numeric_limits<int>::max()
                      text                  int                        convert int to string.
                      text                  numeric                    convert numeric to string.
                      numeric               int                        convert int to double.
                      numeric               text                       set value to nan.

                      others                                           throw an error.

            */

            // read data.
            sqlite3 *db;
            int rc=sqlite3_open(dbFile.c_str(), &db);
            if(rc){
                sqlite3_close(db);
                throw std::runtime_error("Can't open database file: "+dbFile+'\n'+std::string(sqlite3_errmsg(db)));
            }

            // get data.
            sqlite3_stmt *stmt;
            rc=sqlite3_prepare_v2(db, ("SELECT "+cmd).c_str(), -1, &stmt, NULL);
            if (rc!=SQLITE_OK) throw std::runtime_error(std::string(sqlite3_errmsg(db)));

            // save data.
            std::size_t ICnt=0,SCnt=0,DCnt=0;
            std::vector<std::size_t> FieldIndexToIndex;
            std::vector<int> FieldIndexToType;

            m=0;
            while ((rc=sqlite3_step(stmt)) == SQLITE_ROW) {

                // decide each column data type from the 1st row.
                if (m==0) {
                    n=sqlite3_column_count(stmt);
                    FieldIndexToIndex.resize(n);
                    FieldIndexToType.resize(n);

                    for (std::size_t i=0; i<n; ++i) {

                        auto fieldName=std::string((const char *)sqlite3_column_name(stmt,i));


                        switch (sqlite3_column_type(stmt,i)) {
                            case SQLITE_INTEGER : 
                                FieldIndexToIndex[i]=NameToIndex[fieldName]=ICnt++;
                                FieldIndexToType[i]=TypeCheck[fieldName]=SQLite::ISINT;
                                PI.push_back(std::vector<int> ());
                                break;
                            case SQLITE_FLOAT: 
                                FieldIndexToIndex[i]=NameToIndex[fieldName]=DCnt++;
                                FieldIndexToType[i]=TypeCheck[fieldName]=SQLite::ISFLOAT;
                                PF.push_back(std::vector<double> ());
                                break;
                            case SQLITE_TEXT : 
                                FieldIndexToIndex[i]=NameToIndex[fieldName]=SCnt++;
                                FieldIndexToType[i]=TypeCheck[fieldName]=SQLite::ISSTRING;
                                PS.push_back(std::vector<std::string> ());
                                break;
                            default :
                                throw std::runtime_error("Data type error for column: " + fieldName);
                                break;
                        }
                    }
                }

                // Check data type and store the data.

                for (std::size_t i=0; i<n; ++i) {

                    auto type=sqlite3_column_type(stmt,i);
                    switch (FieldIndexToType[i]) {
                        case SQLite::ISINT : 
                            PI[FieldIndexToIndex[i]].push_back((type==SQLITE_INTEGER)?(sqlite3_column_int(stmt,i)):(std::numeric_limits<int>::max()));
                            break;
                        case SQLite::ISFLOAT : 
                            PF[FieldIndexToIndex[i]].push_back((type==SQLITE_TEXT)?(0.0/0.0):(sqlite3_column_double(stmt,i)));
                            break;
                        case SQLite::ISSTRING : 
                            PS[FieldIndexToIndex[i]].push_back((const char *)sqlite3_column_text(stmt,i));
                            break;
                        default:
                            break;
                    }
                }
                ++m;
            }

            if (rc!=SQLITE_DONE) throw std::runtime_error(std::string(sqlite3_errmsg(db)));

            sqlite3_finalize(stmt);
            sqlite3_close(db);
        }


        Select &operator+=(const Select &rhs);

    };

    // Overload operator "+".
    Select operator+(const Select &item1,const Select &item2){
        Select ans(item1);
        ans+=item2;
        return ans;
    }

    Select &Select::operator+=(const Select &rhs){

        // Edge cases.
        if (rhs.NCol()==0) return *this;
        if (NCol()==0) {
            *this=rhs;
            return *this;
        }

        // Edge cases.
        if (rhs.NRow()==0) return *this;
        if (NRow()==0) {
            *this=rhs;
            return *this;
        }

        // Check field names, do they match?
        for (const auto &item:NameToIndex) {
            if (rhs.NameToIndex.find(item.first)==rhs.NameToIndex.end())
                throw std::runtime_error("Can't find field name: "+item.first+" in rhs ...");
            if (rhs.TypeCheck.at(item.first)!=TypeCheck.at(item.first))
                throw std::runtime_error("Can't find same type field : "+item.first+" in rhs ...");
        }


        m+=rhs.m;
        for (const auto &item:rhs.NameToIndex) {

            std::size_t index=item.second;
            std::size_t lhs_index=NameToIndex.at(item.first);
            if (TypeCheck.at(item.first)==SQLite::ISINT)
                PI[lhs_index].insert(PI[lhs_index].end(),rhs.PI[index].begin(),rhs.PI[index].end());
            else if (TypeCheck.at(item.first)==SQLite::ISSTRING)
                PS[lhs_index].insert(PS[lhs_index].end(),rhs.PS[index].begin(),rhs.PS[index].end());
            else
                PF[lhs_index].insert(PF[lhs_index].end(),rhs.PF[index].begin(),rhs.PF[index].end());
        }

        return *this;
    }


    bool CheckTableExists(const std::string &dbFile, const std::string &Table){
        auto res=Select(dbFile,"name from sqlite_master where type='table' and name='"+Table+"'");
        return (res.NRow()==1);
    }




    void Query(const std::string &dbFile, const std::string &cmd){
        sqlite3 *db;
        char *zErrMsg=0;
        int rc=sqlite3_open(dbFile.c_str(), &db);
        if(rc){
            sqlite3_close(db);
            throw std::runtime_error("Can't open database file: "+dbFile+'\n'+std::string(sqlite3_errmsg(db)));
        }

        rc = sqlite3_exec(db, cmd.c_str(), nullptr, nullptr, &zErrMsg);

        if (rc!=SQLITE_OK) {
            sqlite3_free(zErrMsg);
            throw std::runtime_error(std::string(sqlite3_errmsg(db)));
        }
        sqlite3_close(db);
        return;
    }

    void CopyTableStructure(const std::string &dbFile, const std::string &originalTable, const std::string &newTable){
        if (CheckTableExists(dbFile,newTable)) {
            std::cerr << "Table " + newTable + " already exists!" << std::endl;
            return;
        }

        auto createCMD=Select(dbFile,"sql FROM sqlite_master WHERE type='table' AND name='"+originalTable+"'").GetString("sql")[0];
        Query(dbFile,"create table "+newTable+" ("+createCMD.substr(createCMD.find('(')+1));
        return;
    }


    std::vector<bool> IsString(const std::string &dbFile, const std::string &Table, const std::vector<std::string> &fieldnames) {

        if (fieldnames.empty()) return {};

        std::vector<bool> ans(fieldnames.size(),false);
        for (size_t i=0;i<ans.size(); ++i) {
            auto res=Select(dbFile,"type from pragma_table_info('"+Table+"') where name='"+fieldnames[i]+"'");
            ans[i]=(res.GetString("type")[0]=="blob" || res.GetString("type")[0]=="text");
        }
        return ans;
    }


    // number of rows in the table will change.
    // Use NULL as the key word for double data.
    void LoadData(const std::string &dbFile, const std::string &Table,
                  const std::vector<std::string> &fieldnames,
                  const std::vector<std::vector<std::string>> &values,
                  const std::size_t &Batch=10000){

        std::size_t N=fieldnames.size();
        if (N==0) return;
        if (N!=values.size())
            throw std::runtime_error("Values column count != fieldname column count ...");

        std::size_t M=values[0].size();
        for (std::size_t i=1;i<N;++i)
            if (M!=values[i].size())
                throw std::runtime_error("Values row counts are inconsistant...");

        auto types=IsString(dbFile,Table,fieldnames);

        size_t p=0,q=Batch;

        while (p<M) {

            std::string cmd="insert into "+Table+" (";
            for (std::size_t j=0;j<N;++j) cmd+=fieldnames[j]+",";
            cmd.back()=')';
            cmd+=" values ";

            for (std::size_t i=p;i<std::min(M,q);++i) {
                cmd+='(';
                for (std::size_t j=0;j<N;++j)
                    if (types[j]) cmd+="'"+values[j][i]+"',";
                    else cmd+=values[j][i]+",";
                cmd.pop_back();
                cmd+="),";
            }
            cmd.pop_back();

            Query(dbFile,cmd);

            p=q;
            q+=Batch;
        }

        return;
    }


    /***********************************************************************************************
     * 1. if t2 doesn't exist, do nothing.
     * 2. if t1 doesn't exist, copy t2 to t1 (create table t1 like t2; insert t1 select * from t2;)
     * 3. fieldname should be unique in t1 and t2. (fieldname is preferred to be primary key).
     *    -- if field name doesn't exist or is not unique, do nothing.
     *
     * 4. update t1 values from t2, using fieldname as key (row count in t1 will not change):
     *    a. if t1 doesn't have the column(s) in t2, paste the colums(s) from t2 to t1.
     *        -- for rows in t1 doesn't appear in t2, use NULL for new columns(s).
     *        -- for rows in t2 doesn't appear in t1, discard them.
     *    b. if t1 have the column(s) in t2, update the values in t1 using the values from t2.
     *        -- for rows in t1 doesn't appear in t2, do nothing.
    ***********************************************************************************************/

/*
    void UpdateTable(const std::string &t1, const std::string &t2, std::string fieldname) {

        std::transform(fieldname.begin(),fieldname.end(),fieldname.begin(),::tolower);

        auto ID=mysql_init(NULL);

        if (!mysql_real_connect(ID,"localhost","shule","",NULL,0,NULL,0)) {
            std::cerr << mysql_error(ID) << std::endl;
            throw std::runtime_error("Connet failed...");
        }

        // 1. Check t2 exist.
        std::string t2db=t2.substr(0,t2.find_first_of('.'));
        std::string t2table=t2.substr(t2.find_first_of('.')+1);

        std::string cmd="select * from information_schema.tables where table_schema='"+t2db+"' and table_name='"+t2table+"' limit 1";
        if (mysql_real_query(ID,cmd.c_str(),cmd.size())!=0) {
            std::cerr << mysql_error(ID) << std::endl;
            throw std::runtime_error("Select failed...");
        }
        auto res=mysql_store_result(ID);
        if (mysql_num_rows(res)==0) return;


        // 2. Check t1 exist.
        std::string t1db=t1.substr(0,t1.find_first_of('.'));
        std::string t1table=t1.substr(t1.find_first_of('.')+1);
        cmd="select * from information_schema.tables where table_schema='"+t1db+"' and table_name='"+t1table+"' limit 1";
        if (mysql_real_query(ID,cmd.c_str(),cmd.size())!=0) {
            std::cerr << mysql_error(ID) << std::endl;
            throw std::runtime_error("Select failed...");
        }
        res=mysql_store_result(ID);
        if (mysql_num_rows(res)==0) {
            Query("create table "+t1+" like "+t2);
            Query("insert "+t1+" select * from "+t2);
            return;
        }
        mysql_close(ID);


        // 3. check fieldname exists and not repeated.
        auto res1=Select("column_name from information_schema.columns where table_schema='"+t1db+"' and table_name='"+t1table+"'");
        auto res2=Select("column_name,column_type,column_comment from information_schema.columns where table_schema='"+t2db+"' and table_name='"+t2table+"'");
        auto T1N=res1.GetString("column_name");
        auto T2N=res2.GetString("column_name");

        // convert to lower case field naems.
        for (auto &item:T1N) std::transform(item.begin(),item.end(),item.begin(),::tolower);
        for (auto &item:T2N) std::transform(item.begin(),item.end(),item.begin(),::tolower);

        int Cnt=0;
        for (const auto &item: T1N)
            if (item==fieldname) ++Cnt;
        if (Cnt==0)
            throw std::runtime_error("Table "+t1+" doesn't have field called "+fieldname+"...");
        if (Cnt>1)
            throw std::runtime_error("Table "+t1+" have multiple fields called "+fieldname+"...");

        Cnt=0;
        for (const auto &item: T2N)
            if (item==fieldname) ++Cnt;
        if (Cnt==0)
            throw std::runtime_error("Table "+t2+" doesn't have field called "+fieldname+"...");
        if (Cnt>1)
            throw std::runtime_error("Table "+t2+" have multiple fields called "+fieldname+"...");


        // 4. update.
        sort(T1N.begin(),T1N.end());
        auto id=SortWithIndex(T2N.begin(),T2N.end());
        auto T2NN=res2.GetString("column_name");
        auto T2TP=res2.GetString("column_type");
        auto T2CM=res2.GetString("column_comment");
        ReorderUseIndex(T2NN.begin(),T2NN.end(),id);
        ReorderUseIndex(T2TP.begin(),T2TP.end(),id);
        ReorderUseIndex(T2CM.begin(),T2CM.end(),id);

        std::vector<std::string> NeedUpdate(T1N.size());
        auto it=std::set_intersection(T1N.begin(),T1N.end(),T2N.begin(),T2N.end(),NeedUpdate.begin());
        NeedUpdate.resize(it-NeedUpdate.begin());
        NeedUpdate.erase(std::remove(NeedUpdate.begin(),NeedUpdate.end(),fieldname),NeedUpdate.end());

        std::vector<std::string> NeedAdd(T2N.size());
        auto it2=std::set_difference(T2N.begin(),T2N.end(),T1N.begin(),T1N.end(),NeedAdd.begin());
        NeedAdd.resize(it2-NeedAdd.begin());

        // create a tmp table.
        std::string tmptable=t1db+".tMptAble"+std::to_string(getpid());
        Query("create table "+tmptable+" like "+t1);


        // add new columns to tmp table.
        for (const auto &item:NeedAdd) {
            for (std::size_t i=0;i<T2N.size();++i) {
                if (T2N[i]==item){
                    Query("alter table "+tmptable+" add column "+T2NN[i]+" "+T2TP[i]+(T2CM[i].empty()?"":(" comment \""+T2CM[i]+"\"")));
                    break;
                }
            }
        }

        // dump t1, t2 data into tmp table.
        cmd="";
        std::string cmd2="";
        for (const auto &item:res1.GetString("column_name")) {
            cmd2+=(t1+"."+item+",");
            auto item2=item;
            std::transform(item2.begin(),item2.end(),item2.begin(),::tolower);
            if (std::find(NeedUpdate.begin(),NeedUpdate.end(),item2)==NeedUpdate.end())
                cmd+=(t1+"."+item+",");
            else
                cmd+=(t2+"."+item+",");
        }
        for (const auto &item:NeedAdd) {
            cmd+=(t2+"."+item+",");
            cmd2+="NULL,";
        }
        cmd.pop_back();
        cmd2.pop_back();

        Query("insert "+tmptable+" select "+cmd+" from "+t1+" inner join "+t2+" on "+t1+"."+fieldname+"="+t2+"."+fieldname);
        Query("insert "+tmptable+" select "+cmd2+" from "+t1+" left join "+t2+" on "+t1+"."+fieldname+"="+t2+"."+fieldname+" where "+t2+"."+fieldname+" is null");

        // rename tmp table to t1.
        Query("drop table "+t1);
        Query("rename table "+tmptable+" to "+t1);

        return;
    }
*/
}

#endif
