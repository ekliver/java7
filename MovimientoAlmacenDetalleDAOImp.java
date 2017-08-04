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
import java.util.Date;
import sys.dao.MovimientoAlmacenDetalleDAO;
import sys.dao.NotaDespachoDetalleDAO;
import sys.model.AimarMovAlmacenDet;
import sys.model.AvmovMovNotaDespachoDet;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class MovimientoAlmacenDetalleDAOImp implements MovimientoAlmacenDetalleDAO {

    @Override
    public AimarMovAlmacenDet obtenerMovimientoAlmacenDetalle(AimarMovAlmacenDet mAD) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarMovAlmacenDet movimientoAlmacenDetalle = null;
        

        try {
            cn = Service.getConexion();
            String sql = "SELECT IdMovAlmDet "
                    + "FROM AIMAR_MovAlmacenDet "
                    + "WHERE num_movimiento=? AND cod_producto=? AND num_cantidad=? "
                    + "ORDER BY IdMovAlmDet DESC; ";

            ps = cn.prepareStatement(sql);
            ps.setString(1, mAD.getNumMovimiento());
            ps.setString(2, mAD.getCodProducto());
            ps.setInt(3, mAD.getNumCantidad());
            rs = ps.executeQuery();
            if (rs.next()) {
                existe = true;
                movimientoAlmacenDetalle = new AimarMovAlmacenDet();

                movimientoAlmacenDetalle.setIdMovAlmDet(rs.getInt("IdMovAlmDet"));
                
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
        return movimientoAlmacenDetalle;

    }

    @Override
    public void newMovimientoAlmacenDetalle(AimarMovAlmacenDet movimientoAlmacenDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AIMAR_MovAlmacenDet ("
                + "`IdMovAlmCab`,"
                + "`ruc_companyia`, "
                + "`anyo`, "
                + "`cod_establecimiento`, "
                + "`cod_centroc`, "
                + "`cod_area`, "
                + "`cod_almacen`, "
                + "`num_movimiento`, "
                + "`cod_producto`, "
                + "`Cod_TipoProducto`, "
                + "`num_cantidad`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + " `cod_usuario_creacion`,"
                + " `cod_presentacion`,"
                + " `num_cantidad_presentacion`,"
                + " `cod_um`,"
                + " `val_equivalencia`,"
                + " `idNotaDespachoDet`"
                + ") "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");
        movimientoAlmacenDetalle.setFecCreacion(formatoFecha.format(fechaActual));
        movimientoAlmacenDetalle.setHorCreacion(formatoHora.format(fechaActual));
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, movimientoAlmacenDetalle.getIdMovAlmCab());
            ps.setString(2, movimientoAlmacenDetalle.getRucCompanyia());
            ps.setString(3, movimientoAlmacenDetalle.getAnyo());
            ps.setString(4, movimientoAlmacenDetalle.getCodEstablecimiento());
            ps.setInt(5, movimientoAlmacenDetalle.getCodCentroc());
            ps.setInt(6, movimientoAlmacenDetalle.getCodArea());
            ps.setInt(7, movimientoAlmacenDetalle.getCodAlmacen());
            ps.setString(8, movimientoAlmacenDetalle.getNumMovimiento());
            ps.setString(9, movimientoAlmacenDetalle.getCodProducto());
            ps.setString(10, movimientoAlmacenDetalle.getCodTipoProducto());
            ps.setInt(11, movimientoAlmacenDetalle.getNumCantidad());
            ps.setString(12, movimientoAlmacenDetalle.getFecCreacion());
            ps.setString(13, movimientoAlmacenDetalle.getHorCreacion());
            ps.setInt(14, movimientoAlmacenDetalle.getCodUsuarioCreacion());
            ps.setInt(15, movimientoAlmacenDetalle.getCodPresentacion());
            ps.setInt(16, movimientoAlmacenDetalle.getNumCantidadPresentacion());
            ps.setString(17, movimientoAlmacenDetalle.getCodUm());
            ps.setInt(18, movimientoAlmacenDetalle.getValEquivalencia());
            ps.setInt(19, movimientoAlmacenDetalle.getIdNotaDespachoDet());

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
        //
    }

    @Override
    public void updateMovimientoAlmacenDetalle(AimarMovAlmacenDet movimientoAlmacenDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AIMAR_MovAlmacenDet SET "
                + "IdMovAlmCab= ?, \n"
                + "ruc_companyia= ?, \n"
                + "Anyo= ?, \n"
                + "cod_establecimiento= ?, \n"
                + "cod_centroc= ?, \n"
                + "Cod_Area= ?, \n"
                + "cod_almacen= ?, \n"
                + "num_movimiento= ?, \n"
                + "Id_cam= ?, \n"
                + "cod_producto= ?, \n"
                + "Cod_TipoProducto= ?, \n"
                + "Cod_Sector= ?, \n"
                + "num_cantidad= ?, \n"
                + "val_vta_prod= ?, \n"
                + "imp_item= ?, \n"
                + "JCG= ?, \n"
                + "refere= ?, \n"
                + "val_vta_sol= ?, \n"
                + "Imp_Item_Sol= ?, \n"
                + "val_vta_dol= ?, \n"
                + "Imp_Item_Dol= ?, \n"
                + "F3SALDOC= ?, \n"
                + "F3SALDOM= ?, \n"
                + "F3SALPEP= ?, \n"
                + "F2CODALM= ?, \n"
                + "F4FECVAL= ?, \n"
                + "F4CORRELA= ?, \n"
                + "F4CORRELATIVO= ?, \n"
                + "flg_ultimo_costo_prom= ?, \n"
                + "fec_creacion= ?, \n"
                + "hor_creacion= ?, \n"
                + "cod_usuario_creacion= ?, \n"
                + "fec_actualizacion= ?, \n"
                + "hor_actualizacion= ?, \n"
                + "cod_usuario_actualizacion= ?, \n"
                + "Costo_prom_Soles= ?, \n"
                + "Costo_prom_Dolar= ?, \n"
                + "Exportado= ?, \n"
                + "Importado= ?, \n"
                + "fec_importacion= ?, \n"
                + "hor_importacion= ?, \n"
                + "cod_usuario_imp= ?, \n"
                + "nro_orden= ?, \n"
                + "corr_orden= ?, \n"
                + "corr_orden_detalle= ?, \n"
                + "flg_Ajustado= ?, \n"
                + "fec_Ajustado= ?, \n"
                + "hor_Ajustado= ?, \n"
                + "cod_usuario_Ajustado= ?, \n"
                + "cod_presentacion= ?, \n"
                + "num_cantidad_presentacion= ?, \n"
                + "cod_um= ?, \n"
                + "val_equivalencia= ?, \n"
                + "cod_campo_mostrar= ?, \n"
                + "ctd_factura= ?, \n"
                + "cod_transferido= ?, \n"
                + "id_activofijo= ?, \n"
                + "cod_activofijo= ?, "
                + "idNotaDespachoDet= ? "
                + "WHERE IdMovAlmDet= ?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        movimientoAlmacenDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        movimientoAlmacenDetalle.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, movimientoAlmacenDetalle.getIdMovAlmCab());
            ps.setString(2, movimientoAlmacenDetalle.getRucCompanyia());
            ps.setString(3, movimientoAlmacenDetalle.getAnyo());
            ps.setString(4, movimientoAlmacenDetalle.getCodEstablecimiento());
            ps.setInt(5, movimientoAlmacenDetalle.getCodCentroc());
            ps.setInt(6, movimientoAlmacenDetalle.getCodArea());
            ps.setInt(7, movimientoAlmacenDetalle.getCodAlmacen());
            ps.setString(8, movimientoAlmacenDetalle.getNumMovimiento());
            ps.setInt(9, movimientoAlmacenDetalle.getIdCam());
            ps.setString(10, movimientoAlmacenDetalle.getCodProducto());
            ps.setString(11, movimientoAlmacenDetalle.getCodTipoProducto());
            ps.setInt(12, movimientoAlmacenDetalle.getCodSector());
            ps.setInt(13, movimientoAlmacenDetalle.getNumCantidad());
            ps.setDouble(14, movimientoAlmacenDetalle.getValVtaProd());
            ps.setDouble(15, movimientoAlmacenDetalle.getImpItem());
            ps.setString(16, movimientoAlmacenDetalle.getJcg());
            ps.setString(17, movimientoAlmacenDetalle.getRefere());
            ps.setDouble(18, movimientoAlmacenDetalle.getValVtaSol());
            ps.setDouble(19, movimientoAlmacenDetalle.getImpItemSol());
            ps.setDouble(20, movimientoAlmacenDetalle.getValVtaDol());
            ps.setDouble(21, movimientoAlmacenDetalle.getImpItemDol());
            ps.setInt(22, movimientoAlmacenDetalle.getF3saldoc());
            ps.setInt(23, movimientoAlmacenDetalle.getF3saldom());
            ps.setInt(24, movimientoAlmacenDetalle.getF3salpep());
            ps.setString(25, movimientoAlmacenDetalle.getF2codalm());
            ps.setString(26, movimientoAlmacenDetalle.getF4fecval());
            ps.setInt(27, movimientoAlmacenDetalle.getF4correla());
            ps.setInt(28, movimientoAlmacenDetalle.getF4correlativo());
            ps.setInt(29, movimientoAlmacenDetalle.getFlgUltimoCostoProm());
            ps.setString(30, movimientoAlmacenDetalle.getFecCreacion());
            ps.setString(31, movimientoAlmacenDetalle.getHorCreacion());
            ps.setInt(32, movimientoAlmacenDetalle.getCodUsuarioCreacion());
            ps.setString(33, movimientoAlmacenDetalle.getFecActualizacion());
            ps.setString(34, movimientoAlmacenDetalle.getHorActualizacion());
            ps.setInt(35, movimientoAlmacenDetalle.getCodUsuarioActualizacion());
            ps.setInt(36, movimientoAlmacenDetalle.getCostoPromSoles());
            ps.setInt(37, movimientoAlmacenDetalle.getCostoPromDolar());
            ps.setString(38, movimientoAlmacenDetalle.getExportado());
            ps.setString(39, movimientoAlmacenDetalle.getImportado());
            ps.setString(40, movimientoAlmacenDetalle.getFecImportacion());
            ps.setString(41, movimientoAlmacenDetalle.getHorImportacion());
            ps.setInt(42, movimientoAlmacenDetalle.getCodUsuarioImp());
            ps.setString(43, movimientoAlmacenDetalle.getNroOrden());
            ps.setInt(44, movimientoAlmacenDetalle.getCorrOrden());
            ps.setInt(45, movimientoAlmacenDetalle.getCorrOrdenDetalle());
            ps.setString(46, movimientoAlmacenDetalle.getFlgAjustado());
            ps.setString(47, movimientoAlmacenDetalle.getFecAjustado());
            ps.setString(48, movimientoAlmacenDetalle.getHorAjustado());
            ps.setInt(49, movimientoAlmacenDetalle.getCodUsuarioAjustado());
            ps.setInt(50, movimientoAlmacenDetalle.getCodPresentacion());
            ps.setInt(51, movimientoAlmacenDetalle.getNumCantidadPresentacion());
            ps.setString(52, movimientoAlmacenDetalle.getCodUm());
            ps.setInt(53, movimientoAlmacenDetalle.getValEquivalencia());
            ps.setInt(54, movimientoAlmacenDetalle.getCodCampoMostrar());
            ps.setInt(55, movimientoAlmacenDetalle.getCtdFactura());
            ps.setString(56, movimientoAlmacenDetalle.getCodTransferido());
            ps.setInt(57, movimientoAlmacenDetalle.getIdActivofijo());
            ps.setString(58, movimientoAlmacenDetalle.getCodActivofijo());
            ps.setInt(59, movimientoAlmacenDetalle.getIdNotaDespachoDet());
            ps.setInt(60, movimientoAlmacenDetalle.getIdMovAlmDet());

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
    public void updatePrecioMovimientoAlmacenDetalle(AimarMovAlmacenDet movimientoAlmacenDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AIMAR_MovAlmacenDet SET "
                + "val_vta_prod= ?, \n"
                + "imp_item= ?, \n"
                + "val_vta_sol= ?, \n"
                + "Imp_Item_Sol= ?, \n"
                + "val_vta_dol= ?, \n"
                + "Imp_Item_Dol= ?, \n"
                + "fec_actualizacion= ?, \n"
                + "hor_actualizacion= ?, \n"
                + "cod_usuario_actualizacion= ? \n"
                + "WHERE idNotaDespachoDet= ?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        movimientoAlmacenDetalle.setFecActualizacion(formatoFecha.format(fechaActual));
        movimientoAlmacenDetalle.setHorActualizacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            
            ps.setDouble(1, movimientoAlmacenDetalle.getValVtaProd());
            ps.setDouble(2, movimientoAlmacenDetalle.getImpItem());
            ps.setDouble(3, movimientoAlmacenDetalle.getValVtaSol());
            ps.setDouble(4, movimientoAlmacenDetalle.getImpItemSol());
            ps.setDouble(5, movimientoAlmacenDetalle.getValVtaDol());
            ps.setDouble(6, movimientoAlmacenDetalle.getImpItemDol());
            ps.setString(7, movimientoAlmacenDetalle.getFecActualizacion());
            ps.setString(8, movimientoAlmacenDetalle.getHorActualizacion());
            ps.setInt(9, movimientoAlmacenDetalle.getCodUsuarioActualizacion());
            ps.setInt(10, movimientoAlmacenDetalle.getIdNotaDespachoDet());
            
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
    public void deleteMovimientoAlmacenDetalle(AimarMovAlmacenDet movimientoAlmacenDetalle) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AIMAR_MovAlmacenCab WHERE IdMovAlmCab=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, movimientoAlmacenDetalle.getIdMovAlmCab());
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

    
}
