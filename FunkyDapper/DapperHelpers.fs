namespace FunkyDapper
//=====================================================
// Grid Reader
//=====================================================

/// Wraps GridReader from dapper for multi result query helpers
type GridReader = private GridReader of Dapper.SqlMapper.GridReader

/// Simple helpers for working with grid readers
module GridReader =
    
    /// Unwraps the grid reader to expose dappers grid reader
    let value (GridReader rd) = rd

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

    /// Internal helper for working with dapper
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
    
    /// Queries the db with the provided connection 
    /// and returns the result as 'T
    let query<'T> connection sql =
        let  query conn text prms queryType = 
            Dapper.SqlMapper.QueryAsync<'T>(conn, text,  prms, commandType = queryType)
        dapperHelper connection sql query      

    /// Queries the db and returns multiple results
    let queryMultiple connection reader sql = 
        async {
            let query conn text prms queryType = 
                Dapper.SqlMapper.QueryMultipleAsync(conn, text, prms, commandType = queryType) 
            let! result = dapperHelper connection sql query
            return result 
            |> Result.map(fun r -> reader(r|> GridReader))
        }

    /// Exicutes a command on the db
    let execute connection sql =
        let command conn text prms queryType =
            Dapper.SqlMapper.ExecuteAsync(conn, text, prms, commandType = queryType)
        dapperHelper connection sql command
        |> Async.Ignore        