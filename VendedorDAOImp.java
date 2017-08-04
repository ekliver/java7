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
import sys.dao.VendedorDAO;
import sys.model.AgmaeCompanya;
import sys.model.AgmaeVendedor;
import sys.util.Service;

/**
 *
 * @author Pc
 */
public class VendedorDAOImp implements VendedorDAO {

    @Override
    public List<AgmaeVendedor> listarVendedores() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        boolean existe = false;
        AgmaeVendedor vendedor = null;
        List<AgmaeVendedor> lista = new ArrayList<>();
        try {
            cn = Service.getConexion();
            String sql = "SELECT codVendedor, nomVendedor "
                    + "FROM AgmaeVendedor;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()) {
                existe = true;
                vendedor = new AgmaeVendedor();
                vendedor.setCodVendedor(rs.getInt("codVendedor"));
                vendedor.setNomVendedor(rs.getString("nomVendedor"));

                lista.add(vendedor);
            }
            if (!existe) {
                vendedor = null;
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

    @Override
    public AgmaeVendedor consultarObjVendedor(int codVendedor) {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeVendedor vendedor = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT codVendedor, nomVendedor "
                    + "FROM AgmaeVendedor "
                    + "WHERE codVendedor=?;";

            ps = cn.prepareStatement(sql);
            ps.setInt(1, codVendedor);
            rs = ps.executeQuery();

            vendedor = new AgmaeVendedor();
            if (rs.next()) {
                vendedor.setCodVendedor(rs.getInt("codVendedor"));
                vendedor.setNomVendedor(rs.getString("nomVendedor"));

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
        return vendedor;
    }

    
    @Override
    public AgmaeVendedor consultarObjVendedor() {
        Connection cn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        AgmaeVendedor vendedor = null;

        try {
            cn = Service.getConexion();
            String sql = "SELECT codVendedor, nomVendedor "
                    + "FROM AgmaeVendedor "
                    + "WHERE flg_defecto=1;";

            ps = cn.prepareStatement(sql);
            rs = ps.executeQuery();

            vendedor = new AgmaeVendedor();
            if (rs.next()) {
                vendedor.setCodVendedor(rs.getInt("codVendedor"));
                vendedor.setNomVendedor(rs.getString("nomVendedor"));

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
        return vendedor;
    }

}
