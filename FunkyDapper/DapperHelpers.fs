namespace FunkyDapper
open System
open Microsoft.Data.SqlClient

//=====================================================
// Errors
//=====================================================

/// Defines errors that can occur when using the libary
type FuncDapperError =
    internal InvalidSql of string 
    | InvalidParameter of string
    | InvalidConnectionString of string
    | FailedDbCall of string

//=====================================================
// Connections
//=====================================================

/// Defines errors that can occur when using the libary
type Connection = private Connection of SqlConnection

/// Helpers for working with db connections.
module Connection =

    /// Unwraps a connection to get the underlying 
    /// SqlConnection.
    let value (Connection conn) = conn

    /// Creates a new connection from a connection
    /// string.
    let create connStr =
        if connStr |> String.IsNullOrWhiteSpace
        then "Sql connection cannot be null or empty."
            |> InvalidConnectionString
            |> Error
        else
            new SqlConnection(connStr)
            |> Connection
            |> Ok

//=====================================================
// Query Helpers
//=====================================================

/// Simple wrapper around sql text
type SqlText = private SqlText of string

/// Helpers for working with sql text
module SqlText =
    let internal value (SqlText str) = str

    let internal create str =
        if str |> String.IsNullOrWhiteSpace 
        then "Sql text cannot be null."
            |> InvalidSql
            |> Error
        else str
            |> SqlText
            |> Ok
/// Simple wrapper around paramter names 
type ParameterName = private ParameterName of string

/// Helpers for wrking with a parameter name
module ParameterName =

    /// Gets the value of a parameter
    let internal value (ParameterName str) = str

    /// Creates a new parameter
    let internal create (str) =
        if str |> String.IsNullOrWhiteSpace
        then "Sql parameter cannot be null."
            |> InvalidParameter
            |> Error
        else str
            |> ParameterName
            |> Ok

/// Groups the query text to a set of parameters
type ParamertizedSql = private {
    Query:SqlText
    Parameters: (ParameterName * obj) list
}

/// Helpers for wroking with ParameteizedSql
module ParamertizedSql =
    /// Creates a simple parameterized query.
    let internal create query parameters =
        result{
            let! sqlText = query |> SqlText.create
            let! prms = 
                parameters
                |> List.map(fun (name, obj)-> 
                    let pn = name |> ParameterName.create
                    pn |> Result.map(fun n -> (n,obj)))
                |> Result.sequence

            return! 
                if parameters |> List.exists(fun (name,_) -> query.Contains(sprintf "@%s" name))

                then { Query = sqlText; Parameters = prms;}|> Ok
                else
                    "Insure that all parameters are used at least once in the provided sql."
                    |> InvalidParameter
                    |> Error                
        }

    /// Gets the value of a paramertized query by unwrapping it.
    let internal value sql =
        let query = sql.Query |> SqlText.value        
        let prms = 
            sql.Parameters 
            |> List.map(fun (name,obj) -> (name|> ParameterName.value, obj))
            |> dict
        (query,prms)

/// Simple type for working with sql.
type Sql =
    private StoredProcedure of ParamertizedSql
    | PlainSql of ParamertizedSql 
 
/// Helpers for working with sql instances. 
module Sql =
    /// Creates a sql intance that is used to run
    /// simple stored procs without output prams.
    let createStoredProc query parameters =
        ParamertizedSql.create query parameters 
        |>Result.map StoredProcedure

    /// Creates a sql instance to excute plain text 
    /// sql query that does not contain stored procs.
    let createQuery query parameters =
        ParamertizedSql.create query parameters 
        |>Result.map PlainSql

    /// Gets the value of a sql instance as a tuple
    let internal value sql =
        match sql with
        | PlainSql sql ->
            let (query, prms) = ParamertizedSql.value sql
            (query,prms,Data.CommandType.Text|> Nullable)
        | StoredProcedure sql ->
            let (query, prms) = ParamertizedSql.value sql
            (query,prms,Data.CommandType.StoredProcedure|> Nullable)

//=====================================================
// Grid Reader
//=====================================================

/// Wraps GridReader from dapper for multi result query helpers
type GridReader = private GridReader of Dapper.SqlMapper.GridReader

/// Simple helpers for working with grid readers
module GridReader =
    
    /// Unwraps the grid reader to expose dappers grid reader
    let internal value (GridReader rd) = rd

    /// Reads data from the grid reader into a tuple 
    /// of lists for the respective types.
    let readTuple<'T1,'T2> reader =
        use gr = reader |> value
        let r1 = gr.Read<'T1>()|> List.ofSeq
        let r2 = gr.Read<'T2>()|> List.ofSeq
        (r1,r2)

    /// Reads data from the grid reader into a tuple 
    /// of lists for the respective types.
    let readTriple<'T1, 'T2, 'T3> reader =
        use gr = reader |> value
        let r1 = gr.Read<'T1>()|> List.ofSeq
        let r2 = gr.Read<'T2>()|> List.ofSeq
        let r3 = gr.Read<'T3>()|> List.ofSeq
        (r1,r2, r3)

    /// Reads data from the grid reader into a tuple 
    /// of lists for the respective types.
    let readQuad<'T1, 'T2, 'T3, 'T4> reader =
        use gr = reader |> value
        let r1 = gr.Read<'T1>()|> List.ofSeq
        let r2 = gr.Read<'T2>()|> List.ofSeq
        let r3 = gr.Read<'T3>()|> List.ofSeq
        let r4 = gr.Read<'T4>()|> List.ofSeq
        (r1,r2, r3, r4)

//=====================================================
// Grid Reader
//=====================================================
  
 /// Dapper helper methods
module DapperHelpers =
    let private dapperHelper connection sql command =
        async {
            try 
                let conn = connection |> Connection.value
                do! conn.OpenAsync() |> Async.AwaitTask
                let (query, prms, queryType) = Sql.value sql           
                let! res = 
                    command conn query prms queryType
                    |> Async.AwaitTask 
                return res |> Ok
            with ex ->
                return ex.Message 
                |> FailedDbCall 
                |> Error
        }
    
    let query<'T> connection sql =
        let  query conn text prms queryType = 
            Dapper.SqlMapper.QueryAsync<'T>(conn, text, prms, commandType = queryType)
        dapperHelper connection sql query      

    let queryMultiple connection reader sql = 
        async {
            let query conn text prms queryType = 
                Dapper.SqlMapper.QueryMultipleAsync(conn, text, prms, commandType = queryType) 
            let! result = dapperHelper connection sql query
            return result 
            |> Result.map(fun r -> reader(r|> GridReader))
        }

    let execute connection sql =
        let command conn text prms queryType =
            Dapper.SqlMapper.ExecuteAsync(conn, text, prms, commandType = queryType)
        dapperHelper connection sql command
        |> Async.Ignore