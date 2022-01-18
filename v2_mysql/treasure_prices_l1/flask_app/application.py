from flask import Flask, render_template, request, redirect
from flask_sqlalchemy import sqlalchemy

application = Flask(__name__, instance_relative_config=True)
application.jinja_env.globals.update(zip=zip)
application.jinja_env.globals.update(enumerate=enumerate)
application.config.from_pyfile("config.py")
engine = sqlalchemy.create_engine(application.config['SQLALCHEMY_DATABASE_URI'])
db_session = sqlalchemy.orm.scoped_session(sqlalchemy.orm.sessionmaker(autocommit=False,
                                                                        autoflush=False,
                                                                        bind=engine))
Base = sqlalchemy.ext.declarative.declarative_base()
Base.query = db_session.query_property()

itemMappings = {
    "Red FeatherSnow White Feather": "Snow White Feather",
    "Carrage": "Carriage",
    "Silver Penny": "Silver Coin"
}

class L1Treasures(Base):
    __table__ = sqlalchemy.Table(
        'attributes_l1_treasures', 
        Base.metadata,
        autoload=True, 
        autoload_with=engine
    )

class CurrentTokenPrices(Base):
    __table__ = sqlalchemy.Table(
        'current_token_prices', 
        Base.metadata,
        autoload=True, 
        autoload_with=engine
    )
    __mapper_args__ = {
        'primary_key':[__table__.c.id]
    }

class MostRecentSalePrices(Base):
    __table__ = sqlalchemy.Table(
        'most_recent_sale_prices', 
        Base.metadata,
        autoload=True, 
        autoload_with=engine,
    )
    __mapper_args__ = {
        'primary_key':[__table__.c.id]
    }

@application.route("/", methods = ["POST", "GET"])
def index():
    if request.method == "POST":
        bagID = request.form["bag_id"]
        return redirect("/{}".format(bagID))
    else:
        return render_template("index.html")

@application.route("/<bagID>", methods = ["GET"])
def treasures(bagID):
        # Get items in treasure bag
        itemList = db_session.query(L1Treasures).filter(L1Treasures.id==bagID).first().__dict__['item_list'].split(",")
        itemList = [item.replace("'", '').replace('[','').replace(']','').strip() for item in itemList]
        itemList = [itemMappings[item] if item in itemMappings.keys() else item for item in itemList]

        # Get current price of ETH and MAGIC
        priceEth = float(db_session.query(CurrentTokenPrices).all()[0].price_eth_usd)
        priceMagic = float(db_session.query(CurrentTokenPrices).all()[0].price_magic_usd)

        # Get item most recent sale prices
        priceListMagic = []
        for item in itemList:
            priceListMagic.append(db_session.query(MostRecentSalePrices).filter(MostRecentSalePrices.nft_subcategory==item).first().most_recent_sale_price_magic)
        priceListEth = [float(price) / priceEth * priceMagic for price in priceListMagic]
        totalPriceMagic = sum(priceListMagic)
        totalPriceEth = sum(priceListEth)
        priceListMagic = [str(round(price)) for price in priceListMagic]
        priceListEth = [str(round(price,2)) for price in priceListEth]

        db_session.close()

        return render_template(
            "treasures.html", 
            bag_id=bagID,
            item_list=itemList, 
            price_list_magic=priceListMagic, 
            price_list_eth = priceListEth, 
            total_price_magic = str(round(totalPriceMagic)), 
            total_price_eth = str(round(totalPriceEth, 2))
        )

if __name__ == "__main__":
    application.run(port=8080)