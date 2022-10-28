import redis
from bson.objectid import ObjectId
import datetime


conR = redis.Redis(host='redis-13733.c10.us-east-1-2.ec2.cloud.redislabs.com',
                   port=13733,
                   password='bdnr123')

conR.set('user:name', 'priscila')

global mydb
mydb = client.mercadoLivre

######USUÁRIO######


##Insert##
def insertUser(nome, cpf, cep, estado, rua, email, telefone):
    global mydb
    mycol = mydb.usuario
    print("\n######INSERT######")
    mydict = {
        "nome": nome,
        "cpf": cpf,
        "endereco": {
            "cep": cep,
            "estado": estado,
            "rua": rua,
        },
        "telefone": telefone,
        "email": email
    }
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


##Update##
def updateUser(idAlvo, nome, cpf, endereco, email, telefone):
    global mydb
    mycol = mydb.usuario
    print("\n######UPDATE######")
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$set": {
            "nome": nome,
            "cpf": cpf,
            "endereco": endereco,
            "telefone": telefone,
            "email": email
        }
    }
    mycol.update_one(myquery, newValues)
    print(newValues.inserted_id)


##Delete##
def deleteUser(user):
    global mydb
    mycol = mydb.usuario
    print("\n######DELETE######")
    objInstance = ObjectId(user)
    myquery = {"_id": objInstance}
    mycol.delete_one(myquery)
    print(mycol.deleted_id)


######VENDEDOR######


##Insert##
def insertVendedor(nome, email, matricula):
    global mydb
    mycol = mydb.vendedor
    print("\n######INSERT VENDEDOR######")
    mydict = {"nome": nome, "matricula": matricula, "email": email}
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


##Update##
def updateVendedor(idAlvo, nome, email):
    global mydb
    mycol = mydb.vendedor
    print("\n######UPDATE VENDEDOR######")
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {"$set": {"nome": nome, "email": email}}
    mycol.update_one(myquery, newValues)
    print("Vendedor Atualizado com sucesso!")


##Delete##
def deleteVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    print("\n######DELETE######")
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    vendedorColumn.delete_one(myquery)
    print(alvo, "Vendedor deletado com sucesso")


######PRODUTO######


##Insert##
def insertProduto(nomeProdut, preco, descricao, vendedor, quantidade):
    global mydb
    mycol = mydb.produto
    print("\n######INSERT######")
    mydict = {
        "nome": nomeProdut,
        "descricao": descricao,
        "preco": preco,
        "quantidade": quantidade,
        "vendedor": {
            "nome": vendedor,
        }
    }
    x = mycol.insert_one(mydict)
    print(x.inserted_id)


##Update##
def updateProduto(idAlvo, nome, preco, descricao):
    global mydb
    mycol = mydb.produto
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$set": {
            "nome": nome,
            "preco": preco,
            "descricao": descricao
        }
    }
    mycol.update_one(myquery, newValues)
    print(f"Produto {idAlvo} atualizado")


##Delete##
def deleteProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    produtoColumn.delete_one(myquery)
    print(f"Produto {alvo} Deletado")


######COMPRA######


##Insert##
def insertCompra(usuario, pagamento, produto):
    global mydb
    columnCompra = mydb.compra
    print("\n######INSERT COMPRA######")

    compra = {
        "usuario": {
            "nome": usuario
        },
        "produto": [{
            "nome": produto
        }],
        "pagamento": pagamento,
        "data_compra": datetime.datetime.now()
    }
    x = columnCompra.insert_one(compra)
    print(x.inserted_id)


def updateCompraItem(idAlvo, produto):
    global mydb
    columnCompra = mydb.compra
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    updateCompra = {"$addToSet": {"produto": {"nome": produto}}}
    columnCompra.update_one(myquery, updateCompra)


##Update##
def updateCompra(idAlvo, rua, cep, pais):
    global mydb
    mycol = mydb.compra
    objInstance = ObjectId(idAlvo)
    myquery = {"_id": objInstance}
    newValues = {
        "$addToSet": {
            "endereço": {
                "rua": rua,
                "cep": cep,
                "pais": pais
            }
        }
    }
    mycol.update_one(myquery, newValues)
    print("Endereço Atualizado!")


##Delete##
def deleteCompra(alvo):
    global mydb
    mycol = mydb.compra
    objInstance = ObjectId(alvo)
    myquery = {"_id": objInstance}
    mycol.delete_one(myquery)


######QUERYS######


def findSortUser():
    global mydb
    userColumn = mydb.usuario
    mydoc = userColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findQueryUser(alvo):
    global mydb
    mycol = mydb.usuario
    myquery = {"nome": {"$eq": alvo}}
    mydoc = mycol.find(myquery)
    for result in mydoc:
        print(result)


def findQueryVendedor(alvo):
    global mydb
    vendedorColumn = mydb.vendedor
    myquery = {"nome": {"$eq": alvo}}
    mydoc = vendedorColumn.find(myquery)
    for result in mydoc:
        print(result)


def findSortVendedores():
    global mydb
    vendedorColumn = mydb.vendedor
    mydoc = vendedorColumn.find({}, {
        "nome": 1,
        "_id": 1,
    }).sort("nome")
    for result in mydoc:
        print(result)


def findQueryProduto(alvo):
    global mydb
    produtoColumn = mydb.produto
    myquery = {"nome": {"$eq": alvo}}
    mydoc = produtoColumn.find(myquery)
    for result in mydoc:
        print(result)


def findSortCompras():
    global mydb
    comprasColumn = mydb.compra
    mydoc = comprasColumn.find({}, {
        "usuario": {
            "nome": 1
        },
        "produto": {
            "nome": 1,
            "preco": 1,
        }
    }).sort("nome")
    for result in mydoc:
        print(result)


def main():
    inicializacao = True
    print("""
    """)
    while inicializacao:
        select = input("Opção: ")
        if (select == '1'):
            insertUser()
        elif (select == 'x'):
            inicializacao = False


main()

# print(conR.get('user:name'))
