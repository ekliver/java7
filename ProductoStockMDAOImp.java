/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sys.imp;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

import java.util.List;

import sys.dao.ProductoStockMDAO;

import sys.model.AimarStockm;
import sys.model.AvmovMovNotaDespachoDet;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class ProductoStockMDAOImp implements ProductoStockMDAO {

    @Override
    public List<AimarStockm> listarProductoStocks(Date fechaSM) {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarStockm productoStock = null;
        List<AimarStockm> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_Productos.Cod_Ai_Produc AS codProducto, \n"
                    + "AIMAR_Productos.Des_Ai_Produc AS nomProducto, \n"
                    + "AIMAR_Almacen.ruc_companyia AS ruc, \n"
                    + "agmae_companyias.des_companyia AS nomCompanya, \n"
                    + "AIMAR_Almacen.Cod_Ai_Almacen AS codAlmacen, \n"
                    + "AIMAR_Almacen.Nom_Ai_Almacen AS nomAlmacen, \n"
                    + "AGMAE_Establecimiento.COD_establecimiento AS codEstablecimiento, \n"
                    + "AGMAE_Establecimiento.NOM_establecimiento AS nomEstablecimiento, \n"
                    + "AGMAE_Area.Cod_Area AS codArea, \n"
                    + "AGMAE_Area.Des_Abre_Area AS nomArea, \n"
                    + "agmae_centrocosto.cod_centroc AS codCentro, agmae_centrocosto.des_centroc AS nomCentro, \n"
                    + "Sum(IIf([MovCab].[Cod_TipoConcepto]='I',[MovDet].[num_cantidad],\n"
                    + "(-1)*[MovDet].[num_cantidad])) AS stockActual, \n"
                    + "Sum(IIf([MovCab].[fec_movimiento]<=?,\n"
                    + "\n"
                    + "IIf([MovCab].[Cod_TipoConcepto]='I',\n"
                    + "[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad]),0)) AS stockFecha, \n"
                    + "presentacion.cod_presentacion AS codPresentacion, \n"
                    + "presentacion.des_presentacion AS nomPresentacion \n"
                    + "FROM (\n"
                    + "SELECT AIMAR_Productos_Presentacion.cod_producto, AIMAR_Productos_Presentacion.cod_presentacion, \n"
                    + "AIMAR_Presentaciones.des_presentacion \n"
                    + "FROM AIMAR_Presentaciones RIGHT JOIN AIMAR_Productos_Presentacion \n"
                    + "ON AIMAR_Presentaciones.cod_presentacion = AIMAR_Productos_Presentacion.cod_presentacion \n"
                    + "WHERE (((AIMAR_Productos_Presentacion.flg_defecto)='1'))) AS presentacion RIGHT JOIN (agmae_companyias \n"
                    + "INNER JOIN (((AGMAE_Area INNER JOIN (AGMAE_Establecimiento \n"
                    + "INNER JOIN (AIMAR_Almacen INNER JOIN (AIMAR_Productos \n"
                    + "\n"
                    + "INNER JOIN (SELECT * FROM AIMAR_MovAlmacenDet \n"
                    + "UNION ALL SELECT * FROM AIMAR_MovAlmacenDet_Temp)  AS MovDet \n"
                    + "\n"
                    + "ON (AIMAR_Productos.Cod_Ai_Produc = MovDet.cod_producto) \n"
                    + "AND (AIMAR_Productos.ruc_companyia = MovDet.ruc_companyia)) \n"
                    + "ON (AIMAR_Almacen.ruc_companyia = MovDet.ruc_companyia) AND (AIMAR_Almacen.Cod_Ai_Almacen = MovDet.cod_almacen)) \n"
                    + "ON (AGMAE_Establecimiento.ruc_companyia = MovDet.ruc_companyia) \n"
                    + "AND (AGMAE_Establecimiento.COD_establecimiento = MovDet.cod_establecimiento)) \n"
                    + "ON (AGMAE_Area.Cod_Area = MovDet.Cod_Area) \n"
                    + "AND (AGMAE_Area.ruc_companyia = MovDet.ruc_companyia)) \n"
                    + "INNER JOIN agmae_centrocosto ON MovDet.cod_centroc = agmae_centrocosto.cod_centroc) \n"
                    + "\n"
                    + "INNER JOIN (SELECT * FROM AIMAR_MovAlmacenCab \n"
                    + "UNION ALL SELECT * FROM AIMAR_MovAlmacenCab_Temp) AS MovCab\n"
                    + " ON (MovDet.IdMovAlmCab = MovCab.IdMovAlmCab) \n"
                    + "AND (agmae_centrocosto.cod_centroc = MovCab.cod_centroc)) \n"
                    + "ON agmae_companyias.ruc_companyia = MovDet.ruc_companyia) \n"
                    + "ON presentacion.cod_producto = MovDet.cod_producto \n"
                    + "GROUP BY AIMAR_Productos.Cod_Ai_Produc, AIMAR_Productos.Des_Ai_Produc, AIMAR_Almacen.ruc_companyia, agmae_companyias.des_companyia, AIMAR_Almacen.Cod_Ai_Almacen, AIMAR_Almacen.Nom_Ai_Almacen, AGMAE_Establecimiento.COD_establecimiento, AGMAE_Establecimiento.NOM_establecimiento, AGMAE_Area.Cod_Area, AGMAE_Area.Des_Abre_Area, agmae_centrocosto.cod_centroc, agmae_centrocosto.des_centroc, presentacion.cod_presentacion, presentacion.des_presentacion;";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(fechaSM));
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                productoStock = new AimarStockm();

                productoStock.setCodProducto(rs.getString("codProducto"));
                productoStock.setNomProducto(rs.getString("nomProducto"));
                productoStock.setRuc(rs.getString("ruc"));
                productoStock.setNomCompanya(rs.getString("nomCompanya"));
                productoStock.setCodAlmacen(rs.getInt("codAlmacen"));
                productoStock.setNomAlmacen(rs.getString("nomAlmacen"));
                productoStock.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                productoStock.setNomEstablecimiento(rs.getString("nomEstablecimiento"));
                productoStock.setCodArea(rs.getInt("codArea"));
                productoStock.setNomArea(rs.getString("nomArea"));
                productoStock.setCodCentro(rs.getInt("codCentro"));
                productoStock.setNomCentro(rs.getString("nomCentro"));
                productoStock.setStockActual(rs.getInt("stockActual"));
                productoStock.setStockFecha(rs.getInt("stockFecha"));
                productoStock.setCodPresentacion(rs.getInt("codPresentacion"));
                productoStock.setNomPresentacion(rs.getString("nomPresentacion"));

                lista.add(productoStock);
            }
            if (!existe) {
                productoStock = null;
                System.out.println("No se encontro datos");
            }
//              Service.cerrarConexion();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return lista;

    }

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

    @Override
    public AimarStockm consultarObjProductoStock(Date fechaSM, AvmovMovNotaDespachoDet ndt) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        AimarStockm productoStock = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT AIMAR_Productos.Cod_Ai_Produc AS codProducto,\n"
                    + " AIMAR_Productos.Des_Ai_Produc AS nomProducto,\n"
                    + " AIMAR_Almacen.ruc_companyia AS ruc,\n"
                    + " agmae_companyias.des_companyia AS nomCompanya, \n"
                    + "AIMAR_Almacen.Cod_Ai_Almacen AS codAlmacen, "
                    + "AIMAR_Almacen.Nom_Ai_Almacen AS nomAlmacen, "
                    + "AGMAE_Establecimiento.COD_establecimiento AS codEstablecimiento, "
                    + "AGMAE_Establecimiento.NOM_establecimiento AS nomEstablecimiento, "
                    + "AGMAE_Area.Cod_Area AS codArea, AGMAE_Area.Des_Abre_Area AS nomArea, "
                    + "agmae_centrocosto.cod_centroc AS codCentro, "
                    + "agmae_centrocosto.des_centroc AS nomCentro, "
                    + "Sum(IIf([AIMAR_MovAlmacenCab].[Cod_TipoConcepto]='I',[MovDet].[num_cantidad],"
                    + "(-1)*[MovDet].[num_cantidad])) AS stockActual, Sum(IIf([AIMAR_MovAlmacenCab].[fec_movimiento]<=?, "
                    + "IIf([AIMAR_MovAlmacenCab].[Cod_TipoConcepto]='I',"
                    + "[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad]),0)) AS stockFecha\n"
                    + "FROM agmae_companyias INNER JOIN (((AGMAE_Area INNER JOIN (AGMAE_Establecimiento "
                    + "INNER JOIN (AIMAR_Almacen INNER JOIN (AIMAR_Productos "
                    + "INNER JOIN (SELECT * FROM AIMAR_MovAlmacenDet "
                    + "UNION ALL SELECT * FROM AIMAR_MovAlmacenDet_Temp)  AS MovDet "
                    + "ON (AIMAR_Productos.ruc_companyia = MovDet.ruc_companyia) "
                    + "AND (AIMAR_Productos.Cod_Ai_Produc = MovDet.cod_producto)) "
                    + "ON (AIMAR_Almacen.Cod_Ai_Almacen = MovDet.cod_almacen) "
                    + "AND (AIMAR_Almacen.ruc_companyia = MovDet.ruc_companyia)) "
                    + "ON (AGMAE_Establecimiento.COD_establecimiento = MovDet.cod_establecimiento) "
                    + "AND (AGMAE_Establecimiento.ruc_companyia = MovDet.ruc_companyia)) "
                    + "ON (AGMAE_Area.ruc_companyia = MovDet.ruc_companyia) "
                    + "AND (AGMAE_Area.Cod_Area = MovDet.Cod_Area)) "
                    + "INNER JOIN agmae_centrocosto "
                    + "ON MovDet.cod_centroc = agmae_centrocosto.cod_centroc) "
                    + "INNER JOIN AIMAR_MovAlmacenCab "
                    + "ON (agmae_centrocosto.cod_centroc = AIMAR_MovAlmacenCab.cod_centroc) "
                    + "AND (MovDet.IdMovAlmCab = AIMAR_MovAlmacenCab.IdMovAlmCab)) "
                    + "ON agmae_companyias.ruc_companyia = MovDet.ruc_companyia\n"
                    + "GROUP BY AIMAR_Productos.Cod_Ai_Produc, AIMAR_Productos.Des_Ai_Produc, "
                    + "AIMAR_Almacen.ruc_companyia, agmae_companyias.des_companyia, "
                    + "AIMAR_Almacen.Cod_Ai_Almacen, AIMAR_Almacen.Nom_Ai_Almacen, "
                    + "AGMAE_Establecimiento.COD_establecimiento, AGMAE_Establecimiento.NOM_establecimiento, "
                    + "AGMAE_Area.Cod_Area, AGMAE_Area.Des_Abre_Area, agmae_centrocosto.cod_centroc, "
                    + "agmae_centrocosto.des_centroc "
                    + "HAVING (((AIMAR_Productos.Cod_Ai_Produc)=[?]) "
                    + "AND ((AIMAR_Almacen.ruc_companyia)=[?]) "
                    + "AND ((AIMAR_Almacen.Cod_Ai_Almacen)=[?]) "
                    + "AND ((AGMAE_Establecimiento.COD_establecimiento)=[?]) "
                    + "AND ((AGMAE_Area.Cod_Area)=[?]) "
                    + "AND ((agmae_centrocosto.cod_centroc)=[?]));";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(fechaSM));
            ps.setString(2, ndt.getCodProducto());
            ps.setString(3, ndt.getRucCompanyia());
            ps.setInt(4, ndt.getCodAlmacen());
            ps.setString(5, ndt.getCodEstablecimiento());
            ps.setInt(6, ndt.getCodArea());
            ps.setInt(7, ndt.getCodCentroc());

            rs = ps.executeQuery();

            productoStock = new AimarStockm();

            if (rs.next()) {

                productoStock.setCodProducto(rs.getString("codProducto"));
                productoStock.setNomProducto(rs.getString("nomProducto"));
                productoStock.setRuc(rs.getString("ruc"));
                productoStock.setNomCompanya(rs.getString("nomCompanya"));
                productoStock.setCodAlmacen(rs.getInt("codAlmacen"));
                productoStock.setNomAlmacen(rs.getString("nomAlmacen"));
                productoStock.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                productoStock.setNomEstablecimiento(rs.getString("nomEstablecimiento"));
                productoStock.setCodArea(rs.getInt("codArea"));
                productoStock.setNomArea(rs.getString("nomArea"));
                productoStock.setCodCentro(rs.getInt("codCentro"));
                productoStock.setNomCentro(rs.getString("nomCentro"));
                productoStock.setStockActual(rs.getInt("stockActual"));
                productoStock.setStockFecha(rs.getInt("stockFecha"));

            }

//              Service.cerrarConexion();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (cn.isClosed() == false) {
                    cn.close();
                }
            } catch (SQLException e) {
            }
        }
        return productoStock;

    }

}
