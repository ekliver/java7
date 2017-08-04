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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import sys.dao.MovimientoAlmacenDetalleTempDAO;
import sys.model.AgmaeUsuario;
import sys.model.AvmovMovNotaDespachoCab;
import sys.model.AvmovMovNotaDespachoDet;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class MovimientoAlmacenDetalleTempDAOImp implements MovimientoAlmacenDetalleTempDAO {

    @Override
    public List<AvmovMovNotaDespachoDet> listarNotaDespachoDetalles(AvmovMovNotaDespachoCab nd) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AvmovMovNotaDespachoDet notaDespachoDetalle = null;
        List<AvmovMovNotaDespachoDet> lista = new ArrayList<>();

        try {
            cn = Service.getConexion();
            String sql = "SELECT nd.IdMovAlmDet AS codMovDet, nd.IdMovAlmCab AS codMovCab, nd.num_movimiento AS numVale, nd.ruc_Companyia AS ruc, agmae_companyias.des_companyia AS nomCompanya, nd.cod_producto AS codProducto, AIMAR_Productos.Des_Ai_Produc AS nomProducto, nd.cod_presentacion AS codPresentacion, AIMAR_Presentaciones.des_presentacion AS nomPresentacion, AIMAR_Productos_Presentacion.val_equivalencia AS numPresentacionEquivalencia, AIMAR_Productos.Cod_Medida AS codMedida, AIMAR_Unidad_Medida.NomMed_AI_UniMed AS nomMedida, nd.num_cantidad AS ctdMov, stock.stockActual AS sActual, stock.stockFecha AS sFecha, nd.cod_establecimiento AS codEstablecimiento, nd.cod_area AS codArea, nd.cod_almacen AS codAlmacen, nd.cod_centroc AS codCentro, nd.num_cantidad_presentacion AS numCantidadPresentacion\n"
                    + "FROM (AIMAR_Unidad_Medida RIGHT JOIN (AIMAR_Productos_Presentacion RIGHT JOIN ((AIMAR_Productos RIGHT JOIN (AIMAR_MovAlmacenDet_Temp AS nd LEFT JOIN agmae_companyias ON nd.ruc_Companyia = agmae_companyias.ruc_companyia) ON (AIMAR_Productos.Cod_Ai_Produc = nd.cod_producto) AND (AIMAR_Productos.ruc_companyia = nd.ruc_Companyia)) LEFT JOIN AIMAR_Presentaciones ON nd.cod_presentacion = AIMAR_Presentaciones.cod_presentacion) ON (AIMAR_Productos_Presentacion.cod_producto = nd.cod_producto) AND (AIMAR_Productos_Presentacion.cod_presentacion = nd.cod_presentacion)) ON (AIMAR_Unidad_Medida.ruc_companyia = AIMAR_Productos.ruc_companyia) AND (AIMAR_Unidad_Medida.CodMed_AI_UniMed = AIMAR_Productos.Cod_Medida)) LEFT JOIN (SELECT Sum(IIf([MovCab].[Cod_TipoConcepto]='I', \n"
                    + "[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad])) AS stockActual, \n"
                    + "Sum(IIf([MovCab].[fec_movimiento]<=?,IIf ([MovCab].[Cod_TipoConcepto]='I',[MovDet].[num_cantidad],(-1)*[MovDet].[num_cantidad]),0)) AS stockFecha, \n"
                    + "MovDet.ruc_companyia AS codCompanya, \n"
                    + "MovDet.cod_establecimiento AS codEstablecimiento, \n"
                    + "MovDet.cod_centroc AS codCentro,  \n"
                    + "MovDet.Cod_Area AS codArea, \n"
                    + "MovDet.cod_almacen AS codAlmacen, \n"
                    + "MovDet.cod_producto AS codProducto \n"
                    + "FROM (SELECT * FROM AIMAR_MovAlmacenDet UNION ALL SELECT * FROM AIMAR_MovAlmacenDet_Temp) AS MovDet  \n"
                    + "INNER JOIN (SELECT * FROM AIMAR_MovAlmacenCab  \n"
                    + "UNION ALL SELECT * FROM AIMAR_MovAlmacenCab_Temp) AS MovCab ON MovDet.IdMovAlmCab = MovCab.IdMovAlmCab \n"
                    + "GROUP BY MovDet.ruc_companyia, MovDet.cod_establecimiento, MovDet.cod_centroc, MovDet.Cod_Area, MovDet.cod_almacen, MovDet.cod_producto  \n"
                    + ")  AS stock ON (nd.cod_centroc = stock.codCentro) AND (nd.ruc_Companyia = stock.codCompanya) AND (nd.cod_producto = stock.codProducto) AND (nd.cod_area = stock.codArea) AND (nd.cod_almacen = stock.codAlmacen) AND (nd.cod_establecimiento = stock.codEstablecimiento) "
                    + "WHERE (((nd.num_movimiento)=?));";

            ps = cn.prepareStatement(sql);
            ps.setDate(1, convertJavaDateToSqlDate(nd.getFecVale()));
            ps.setString(2, nd.getNumVale());
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespachoDetalle = new AvmovMovNotaDespachoDet();

                notaDespachoDetalle.setIdMovValeProducto(rs.getInt("codMovDet"));
                notaDespachoDetalle.setIdMovValeCab(rs.getInt("codMovCab"));
                notaDespachoDetalle.setRucCompanyia(rs.getString("ruc"));
                notaDespachoDetalle.setNumVale(rs.getString("numVale"));
                notaDespachoDetalle.setCodEstablecimiento(rs.getString("codEstablecimiento"));
                notaDespachoDetalle.setCodCentroc(rs.getInt("codCentro"));
                notaDespachoDetalle.setCodArea(rs.getInt("codArea"));
                notaDespachoDetalle.setCodAlmacen(rs.getInt("codAlmacen"));
                notaDespachoDetalle.setCodProducto(rs.getString("codProducto"));
                notaDespachoDetalle.setCodPresentacion(rs.getInt("codPresentacion"));

                notaDespachoDetalle.setNomCompanya(rs.getString("nomCompanya"));
                notaDespachoDetalle.setNomProducto(rs.getString("nomProducto"));
                notaDespachoDetalle.setNomPresentacion(rs.getString("nomPresentacion"));
                notaDespachoDetalle.setNumPresentacionEquivalencia(rs.getInt("numPresentacionEquivalencia"));
                notaDespachoDetalle.setNumCantidadPresentacion(0);
                notaDespachoDetalle.setCodMedida(rs.getString("codMedida"));
                notaDespachoDetalle.setNomMedida(rs.getString("nomMedida"));
                notaDespachoDetalle.setStockActual(rs.getInt("sActual"));
                notaDespachoDetalle.setStockFecha(rs.getInt("sFecha"));

                notaDespachoDetalle.setCtdMovimiento(rs.getInt("ctdMov"));
                notaDespachoDetalle.setNumCantidadPresentacion(rs.getInt("numCantidadPresentacion"));

                lista.add(notaDespachoDetalle);
            }
            if (!existe) {
                lista = null;
                System.out.println("No se encontro datos");
            }
//  Service.cerrarConexion();
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

    @Override
    public void newMovimientoAlmacenDetalleTemp(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AIMAR_MovAlmacenDet_Temp ("
                + "`IdMovAlmCab`, "
                + "`ruc_companyia`, "
                + "`cod_establecimiento`, "
                + "`cod_centroc`, "
                + "`cod_area`, "
                + "`cod_almacen`, "
                + "`num_movimiento`, "
                + "`cod_producto`, "
                + "`num_cantidad`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + " `cod_usuario_creacion`,"
                + " `cod_presentacion`, "
                + " `num_cantidad_presentacion`"
                + ") "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespachoDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        notaDespachoDetalle.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);

            ps.setInt(1, notaDespachoDetalle.getIdMovValeCab());
            ps.setString(2, notaDespachoDetalle.getRucCompanyia());
            ps.setString(3, notaDespachoDetalle.getCodEstablecimiento());
            ps.setInt(4, notaDespachoDetalle.getCodCentroc());
            ps.setInt(5, notaDespachoDetalle.getCodArea());
            ps.setInt(6, notaDespachoDetalle.getCodAlmacen());
            ps.setString(7, notaDespachoDetalle.getNumVale());
            ps.setString(8, notaDespachoDetalle.getCodProducto());
            ps.setInt(9, notaDespachoDetalle.getCtdMovimiento());
            ps.setString(10, notaDespachoDetalle.getFecCreacion());
            ps.setString(11, notaDespachoDetalle.getHorCreacion());
            ps.setInt(12, notaDespachoDetalle.getCodUsuarioCreacion());
            ps.setInt(13, notaDespachoDetalle.getCodPresentacion());
            ps.setInt(14, notaDespachoDetalle.getNumCantidadPresentacion());
            ps.executeUpdate();
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
    }

    @Override
    public void updateMovimientoAlmacenDetalleTemp(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AIMAR_MovAlmacenDet_Temp SET "
                + "ruc_companyia= ?, \n"
                + "cod_establecimiento= ?, \n"
                + "cod_centroc= ?, \n"
                + "Cod_Area= ?, \n"
                + "cod_almacen= ?, \n"
                + "num_movimiento= ?, \n"
                + "cod_producto= ?, \n"
                + "num_cantidad= ?, \n"
                + "fec_actualizacion= ?, \n"
                + "hor_actualizacion= ?, \n"
                + "cod_usuario_actualizacion= ?, \n"
                + "cod_presentacion= ?, "
                + "num_cantidad_presentacion=? "
                + "WHERE IdMovAlmDet= ?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespachoDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        notaDespachoDetalle.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespachoDetalle.getRucCompanyia());
            ps.setString(2, notaDespachoDetalle.getCodEstablecimiento());
            ps.setInt(3, notaDespachoDetalle.getCodCentroc());
            ps.setInt(4, notaDespachoDetalle.getCodArea());
            ps.setInt(5, notaDespachoDetalle.getCodAlmacen());
            ps.setString(6, notaDespachoDetalle.getNumVale());
            ps.setString(7, notaDespachoDetalle.getCodProducto());
            ps.setInt(8, notaDespachoDetalle.getCtdMovimiento());
            ps.setString(9, notaDespachoDetalle.getFecActualizacion());
            ps.setString(10, notaDespachoDetalle.getHorActualizacion());
            ps.setInt(11, notaDespachoDetalle.getCodUsuarioActualizacion());
            ps.setInt(12, notaDespachoDetalle.getCodPresentacion());
            ps.setInt(13, notaDespachoDetalle.getNumCantidadPresentacion());
            ps.setInt(14, notaDespachoDetalle.getIdMovValeProducto());

            ps.executeUpdate();
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

    }

    @Override
    public void deleteMovimientoAlmacenDetalleTemp(AvmovMovNotaDespachoDet notaDespachoDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AIMAR_MovAlmacenDet_Temp "
                + "WHERE IdMovAlmDet=?; ";

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, notaDespachoDetalle.getIdMovValeProducto());

            ps.executeUpdate();
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

    }

    @Override
    public void deleteAllMovimientoAlmacenDetalleTemp(AgmaeUsuario usuario) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AIMAR_MovAlmacenDet_Temp "
                + "WHERE cod_usuario_creacion=? ";

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, usuario.getCodUsuario());
            ps.executeUpdate();
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

    }

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

}
