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
import sys.dao.ClienteDAO;
import sys.dao.MovimientoAlmacenDAO;
import sys.dao.MovimientoAlmacenTempDAO;
import sys.model.AimarMovAlmacenCab;
import sys.model.AvmovMovNotaDespachoCab;
import sys.util.Service;

public class MovimientoAlmacenTempDAOImp implements MovimientoAlmacenTempDAO {

    @Override
    public List<AimarMovAlmacenCab> listarMovimientosAlmacen() {

        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AimarMovAlmacenCab notaDespacho = null;
        List<AimarMovAlmacenCab> lista = new ArrayList<>();
        ClienteDAO cDAO = new ClienteDAOImp();

        try {
            cn = Service.getConexion();
            String sql = "SELECT IdMovAlmCab, ruc_companyia, Anyo, cod_establecimiento, "
                    + "cod_centroc, Cod_Area, cod_almacen, Cod_TipoProducto, "
                    + "num_movimiento, Cod_TipoConcepto, num_documento, cod_persona, "
                    + "fec_movimiento, cod_concepto, cod_documento, tip_cambio, "
                    + "cod_moneda, F1TIPPRV, cod_establecimiento_des, cod_centroc_des, "
                    + "cod_almacen_des, cod_area_des, num_movimiento_des, num_factura, "
                    + "num_orden, num_imp, partida, F4ORDTRA, F4FECULT, F2CODUSE, "
                    + "Refere, F4CONTABLE, F4CORRELATIVO, flg_Origen, fec_creacion, "
                    + "hor_creacion, cod_usuario_creacion, fec_actualizacion, "
                    + "hor_actualizacion, cod_usuario_actualizacion, Exportado, "
                    + "Importado, fec_importacion, hor_importacion, cod_usuario_imp, "
                    + "num_mov_importado, flg_formVoucher, cod_sub_concepto, "
                    + "flg_AsientoContable, id_activofijo, fec_asiento, anno_asiento, "
                    + "mes_asiento, tipo_asiento, num_asiento, fec_creacion_asiento, "
                    + "hor_creacion_asiento, cod_usuario_asiento, cod_centron, "
                    + "des_Glosa, flg_transcodigos, flg_editando, des_observacion, "
                    + "cod_usuario_editando, fec_usuario_editando, hor_usuario_editando "
                    + "FROM AIMAR_MovAlmacenCab; ";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                notaDespacho = new AimarMovAlmacenCab();

                notaDespacho.setIdMovAlmCab(rs.getInt("IdMovAlmCab"));
                notaDespacho.setRucCompanyia(rs.getString("ruc_companyia"));
                notaDespacho.setAnyo(rs.getString("Anyo"));
                notaDespacho.setCodEstablecimiento(rs.getString("cod_establecimiento"));
                notaDespacho.setCodCentroc(rs.getInt("cod_centroc"));
                notaDespacho.setCodArea(rs.getInt("Cod_Area"));
                notaDespacho.setCodAlmacen(rs.getInt("cod_almacen"));
                notaDespacho.setCodTipoProducto(rs.getString("Cod_TipoProducto"));
                notaDespacho.setNumMovimiento(rs.getString("num_movimiento"));
                notaDespacho.setCodTipoConcepto(rs.getString("Cod_TipoConcepto"));
                notaDespacho.setNumDocumento(rs.getString("num_documento"));
                notaDespacho.setCodPersona(rs.getInt("cod_persona"));
                notaDespacho.setFecMovimiento(rs.getDate("fec_movimiento"));
                notaDespacho.setCodConcepto(rs.getString("cod_concepto"));
                notaDespacho.setCodDocumento(rs.getString("cod_documento"));
                notaDespacho.setTipCambio(rs.getDouble("tip_cambio"));
                notaDespacho.setCodMoneda(rs.getString("cod_moneda"));
                notaDespacho.setF1tipprv(rs.getString("F1TIPPRV"));
                notaDespacho.setCodEstablecimientoDes(rs.getString("cod_establecimiento_des"));
                notaDespacho.setCodCentrocDes(rs.getInt("cod_centroc_des"));
                notaDespacho.setCodAlmacenDes(rs.getInt("cod_almacen_des"));
                notaDespacho.setCodAreaDes(rs.getInt("cod_area_des"));
                notaDespacho.setNumMovimientoDes(rs.getString("num_movimiento_des"));
                notaDespacho.setNumFactura(rs.getString("num_factura"));
                notaDespacho.setNumOrden(rs.getString("num_orden"));
                notaDespacho.setNumImp(rs.getString("num_imp"));
                notaDespacho.setPartida(rs.getString("partida"));
                notaDespacho.setF4ordtra(rs.getString("F4ORDTRA"));
                notaDespacho.setF4fecult(rs.getString("F4FECULT"));
                notaDespacho.setF2coduse(rs.getString("F2CODUSE"));
                notaDespacho.setRefere(rs.getString("Refere"));
                notaDespacho.setF4contable(rs.getString("F4CONTABLE"));
                notaDespacho.setF4correlativo(rs.getInt("F4CORRELATIVO"));
                notaDespacho.setFlgOrigen(rs.getString("flg_Origen"));
                notaDespacho.setFecCreacion(rs.getString("fec_creacion"));
                notaDespacho.setHorCreacion(rs.getString("hor_creacion"));
                notaDespacho.setCodUsuarioCreacion(rs.getInt("cod_usuario_creacion"));
                notaDespacho.setFecActualizacion(rs.getString("fec_actualizacion"));
                notaDespacho.setHorActualizacion(rs.getString("hor_actualizacion"));
                notaDespacho.setCodUsuarioActualizacion(rs.getInt("cod_usuario_actualizacion"));
                notaDespacho.setExportado(rs.getString("Exportado"));
                notaDespacho.setImportado(rs.getString("Importado"));
                notaDespacho.setFecImportacion(rs.getString("fec_importacion"));
                notaDespacho.setHorImportacion(rs.getString("hor_importacion"));
                notaDespacho.setCodUsuarioImp(rs.getInt("cod_usuario_imp"));
                notaDespacho.setNumMovImportado(rs.getString("num_mov_importado"));
                notaDespacho.setFlgFormVoucher(rs.getString("flg_formVoucher"));
                notaDespacho.setCodSubConcepto(rs.getString("cod_sub_concepto"));
                notaDespacho.setFlgAsientoContable(rs.getString("flg_AsientoContable"));
                notaDespacho.setIdActivofijo(rs.getInt("id_activofijo"));
                notaDespacho.setFecAsiento(rs.getString("fec_asiento"));
                notaDespacho.setAnnoAsiento(rs.getString("anno_asiento"));
                notaDespacho.setMesAsiento(rs.getString("mes_asiento"));
                notaDespacho.setTipoAsiento(rs.getString("tipo_asiento"));
                notaDespacho.setNumAsiento(rs.getInt("num_asiento"));
                notaDespacho.setFecCreacionAsiento(rs.getString("fec_creacion_asiento"));
                notaDespacho.setHorCreacionAsiento(rs.getString("hor_creacion_asiento"));
                notaDespacho.setCodUsuarioAsiento(rs.getInt("cod_usuario_asiento"));
                notaDespacho.setCodCentron(rs.getString("cod_centron"));
                notaDespacho.setDesGlosa(rs.getString("des_Glosa"));
                notaDespacho.setFlgTranscodigos(rs.getString("flg_transcodigos"));
                notaDespacho.setFlgEditando(rs.getString("flg_editando"));
                notaDespacho.setDesObservacion(rs.getString("des_observacion"));
                notaDespacho.setCodUsuarioEditando(rs.getInt("cod_usuario_editando"));
                notaDespacho.setFecUsuarioEditando(rs.getString("fec_usuario_editando"));
                notaDespacho.setHorUsuarioEditando(rs.getString("hor_usuario_editando"));
                notaDespacho.setAgmaePersona(cDAO.consultarObjCliente(notaDespacho.getCodPersona()));

                lista.add(notaDespacho);
            }
            if (!existe) {
                notaDespacho = null;
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

    @Override
    public void newMovimientoAlmacen(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "INSERT INTO AIMAR_MovAlmacenCab_Temp ("
                + "`ruc_companyia`, "
                + "`cod_establecimiento`, "
                + "`cod_centroc`, "
                + "`cod_area`, "
                + "`cod_almacen`, "
                + "`num_movimiento`, "
                + "`fec_movimiento`, "
                + "`fec_creacion`, "
                + "`hor_creacion`, "
                + " `cod_usuario_creacion`"
                + ") "
                + "VALUES(?,?,?,?,?,?,?,?,?,?); ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespacho.setFecCreacion(formatoFecha.format(fechaActual));
        notaDespacho.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setString(2, notaDespacho.getCodEstablecimiento());
            ps.setInt(3, notaDespacho.getCodCentroc());
            ps.setInt(4, notaDespacho.getCodArea());
            ps.setInt(5, notaDespacho.getCodAlmacen());
            ps.setString(6, notaDespacho.getNumVale());
            ps.setDate(7, convertJavaDateToSqlDate(notaDespacho.getFecVale()));
            ps.setString(8, notaDespacho.getFecCreacion());
            ps.setString(9, notaDespacho.getHorCreacion());
            ps.setInt(10, notaDespacho.getCodUsuarioCreacion());

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
    public void updateMovimientoAlmacen(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "UPDATE AIMAR_MovAlmacenCab_Temp SET "
                + "ruc_companyia=?, \n"
                + "cod_establecimiento=?, \n"
                + "cod_centroc=?, \n"
                + "Cod_Area=?, \n"
                + "cod_almacen=?, \n"
                + "cod_persona=?, \n"
                + "fec_movimiento=?, \n"
                + "fec_creacion=?, \n"
                + "hor_creacion=?, \n"
                + "cod_usuario_creacion=? \n"
                + "WHERE IdMovAlmCab=?; ";

        Date fechaActual = new Date();
        SimpleDateFormat formatoFecha = new SimpleDateFormat("dd/MM/yyyy");
        SimpleDateFormat formatoHora = new SimpleDateFormat("HH:mm:ss");

        notaDespacho.setFecCreacion(formatoFecha.format(fechaActual));
        notaDespacho.setHorCreacion(formatoHora.format(fechaActual));

        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getRucCompanyia());
            ps.setString(2, notaDespacho.getCodEstablecimiento());
            ps.setInt(3, notaDespacho.getCodCentroc());
            ps.setInt(4, notaDespacho.getCodArea());
            ps.setInt(5, notaDespacho.getCodAlmacen());
            ps.setInt(6, notaDespacho.getCodPersona());
            ps.setDate(7, convertJavaDateToSqlDate(notaDespacho.getFecVale()));
            ps.setString(8, notaDespacho.getFecCreacion());
            ps.setString(9, notaDespacho.getHorCreacion());
            ps.setInt(10, notaDespacho.getCodUsuarioCreacion());
            ps.setInt(11, notaDespacho.getIdMovValeCab());
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
    public void deleteMovimientoAlmacen(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AIMAR_MovAlmacenCab_Temp WHERE cod_usuario_creacion=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setInt(1, notaDespacho.getCodUsuarioCreacion());
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
    public void deleteAllMovimientoAlmacen() {
        Connection cn = null;
        PreparedStatement ps = null;
        String sql = "DELETE FROM AIMAR_MovAlmacenCab_Temp";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
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
    public AvmovMovNotaDespachoCab obtenerMovimientoAlmacen(AvmovMovNotaDespachoCab notaDespacho) {
        Connection cn = null;
        AvmovMovNotaDespachoCab nd = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        String sql = "SELECT IdMovAlmCab FROM AIMAR_MovAlmacenCab_Temp WHERE num_movimiento=?;";
        try {
            cn = Service.getConexion();
            ps = cn.prepareStatement(sql);
            ps.setString(1, notaDespacho.getNumVale());

            rs = ps.executeQuery();

            if (rs.next()) {
                nd = new AvmovMovNotaDespachoCab();
                nd.setIdMovValeCab(rs.getInt("IdMovAlmCab"));

            }

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
        return nd;
    }

    public java.sql.Date convertJavaDateToSqlDate(java.util.Date date) {
        return new java.sql.Date(date.getTime());
    }

}
