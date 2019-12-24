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
    let value (SqlText str) = str

    let create str =
        if str |> String.IsNullOrWhiteSpace 
        then "Sql text cannot be null."
            |> InvalidSql
            |> Error
        else str
            |> SqlText
            |> Ok

    let append sqlTextLeft sqlTextRight =
        let left = sqlTextLeft |> value
        let right = sqlTextRight |> value
        left + "\n" + right
        |> SqlText

/// Simple wrapper around paramter names 
type ParameterName = private ParameterName of string

/// Helpers for wrking with a parameter name
module ParameterName =

    /// Gets the value of a parameter
    let value (ParameterName str) = str

    /// Creates a new parameter
    let create (str) =
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
    let create query parameters =
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
    let value sql =
        let query = sql.Query |> SqlText.value        
        let prms = 
            sql.Parameters 
            |> List.map(fun (name,obj) -> (name|> ParameterName.value, obj))
            |> dict
        (query,prms)

    /// Appends to queries safely
    let append sqlLeft sqlRight =
        let sqlText = SqlText.append sqlLeft.Query sqlRight.Query
        let prms = 
            sqlLeft.Parameters @ sqlRight.Parameters
            |> List.distinct
        {
            Query = sqlText
            Parameters = prms
        }

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
    let value sql =
        match sql with
        | PlainSql sql ->
            let (query, prms) = ParamertizedSql.value sql
            (query,prms,Data.CommandType.Text|> Nullable)
        | StoredProcedure sql ->
            let (query, prms) = ParamertizedSql.value sql
            (query,prms,Data.CommandType.StoredProcedure|> Nullable)

    // Appends two queries if possible
    let append sqlLeft sqlRight =
        match (sqlLeft, sqlRight) with
        | (PlainSql left, PlainSql right) -> ParamertizedSql.append left right |> Ok
        | (StoredProcedure left, StoredProcedure right) -> ParamertizedSql.append left right  |> Ok
        | _ -> "Currently we only support append on queies of the same type." |> Error
