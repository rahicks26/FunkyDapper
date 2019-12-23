namespace FunkyDapper.Factories

open FunkyDapper

type IConnectionFactory =
    abstract member fromConnectionString: string -> Result<Connection,FuncDapperError>
    
type DefaultConnectionFactory =
    interface IConnectionFactory with
         member _.fromConnectionString str = Connection.create str