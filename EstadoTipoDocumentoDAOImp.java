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
import java.util.List;
import sys.dao.EstadoTipoDocumentoDAO;
import sys.model.AgmaeEstadoTipoDocumento;
import sys.model.AimarEmpresaTransporte;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class EstadoTipoDocumentoDAOImp implements EstadoTipoDocumentoDAO {

    @Override
    public List<AgmaeEstadoTipoDocumento> listarEstadoTipoDocumento(String codTipoDocumento) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeEstadoTipoDocumento estado = null;
        List<AgmaeEstadoTipoDocumento> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT AGMAE_ESTADO_TIPO_DOCUMENTO.cod_documento, "
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.cod_estado, "
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.des_estado, "
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_print, "
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_edit, "
                    + "AGMAE_ESTADO_TIPO_DOCUMENTO.flg_button_del "
                    + "FROM AGMAE_ESTADO_TIPO_DOCUMENTO "
                    + "WHERE AGMAE_ESTADO_TIPO_DOCUMENTO.cod_documento=?;";

            ps = cn.prepareStatement(sql);
            ps.setString(1, codTipoDocumento);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                estado = new AgmaeEstadoTipoDocumento();
                estado.setCodDocumento(rs.getString(1));
                estado.setCodEstado(rs.getString(2));
                estado.setDesEstado(rs.getString(3));
                estado.setFlgButtonPrint(rs.getString(4));
                estado.setFlgButtonEdit(rs.getString(5));
                estado.setFlgButtonDel(rs.getString(6));

                lista.add(estado);
            }
            if (!existe) {
                estado = null;
                System.out.println("No se encontro datos");
            }
//            Service.cerrarConexion();
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

}
